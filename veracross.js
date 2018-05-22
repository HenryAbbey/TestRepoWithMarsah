var async = require("async"),
    request = require("request"),
    util = require("./util.js"),
    lastModified = "",
    latestModified = "",
    self = module.exports = {
        data: require("./data.js"),
        namespace: "veracross",
        getRecords: (entity, options, keyName, cb) => {
            async.waterfall([
                cb => { self.data.get(self.namespace, "latest/" + entity, (snapshot) => {
                    latestModified = lastModified = snapshot.val() ? snapshot.val() : "";
                    cb()
                }) },
                cb => { self.getRecentRecords(entity, options, keyName, cb) },
                cb => { self.data.set(self.namespace, "latest/" + entity, latestModified, cb) },
            ], cb)
        },
        request: (endPoint, cb) => {
            request( { url: "https://" + self.data.config[self.namespace].username + ":" + util.decrypt(self.data.config[self.namespace].pHash) + "@" + self.data.config[self.namespace].url + endPoint, json: true }, (err, response, result) => {
                if (response.statusCode === 200 || response.statusCode === 400) return cb(null, response, result)
                if (response.statusCode === 429) {
                    var delay = response.headers["x-rate-limit-reset"]
                    console.log("Retrying in " + response.headers["x-rate-limit-reset"] + " seconds...")
                    return setTimeout(() => { self.request(endPoint, cb) }, response.headers["x-rate-limit-reset"] * 1000)
                }
                cb(err ? err : "Response Status Code: " + response.statusCode)
            })
        },
        logProgress: (entity, count, page, totalPages) => {
            if (count) console.log(self.namespace + "." + entity + " (" + count + ", page " + page + " of " + totalPages + ")")
        },
        getRelationships: (entity, household, cb) => {
            if (entity != "households") return process.nextTick(() => { cb() })
            var arr = []
            household.relationships = {}
            if (household.parents) for (var i in household.parents) arr.push({person_pk: household.parents[i].person_pk, endPoint: "parents/" + household.parents[i].person_pk + "/relationships.json"})
            if (household.students) for (var i in household.students) arr.push({person_pk: household.students[i].person_pk, endPoint: "students/" + household.students[i].person_pk + "/relationships.json"})
            async.eachSeries(
                arr, 
                (item, cb) => {
                    self.request(item.endPoint, (err, response, result) => {
                        if (err) return cb(err)
                        if (result) household.relationships[item.person_pk] = result
                        cb()
                    })
                }, cb)
        },
        getRecentRecords: (entity, options, keyName, cb) => {
            var page = totalPages = batchCount = countRemaining = 0
            async.doWhilst(
                (cb) => { 
                    self.logProgress(entity, countRemaining, ++page, totalPages)
                    self.request(entity + ".json?" + (options ? options + "&" : "test") + (lastModified ? "updated_after=" + lastModified.substring(0, 10) + "&" : "") + "page=" + page, (err, response, result) => {
                        if (page == 1) self.logProgress(entity, countRemaining = response.headers["x-total-count"], page, totalPages = Math.ceil(response.headers["x-total-count"]/100))
                        if (err) return cb(err)
                        async.eachSeries(
                            result, 
                            (item, cb) => {
                                if (item.update_date > latestModified) latestModified = item.update_date
                                self.updateRecord(entity, keyName, item, () => {
                                    if (entity != "people" || !item.household_fk) return cb()
                                    self.data.get(self.namespace, "households/" + item.household_fk + "/relationships/" + item[keyName], (snapshot) => {
                                        if (snapshot.val()) return cb()
                                        self.request("households/" + item.household_fk + ".json", (err, response, result) => {
                                            if (!result) return cb()
                                            self.updateRecord("households", "household_pk", result, cb)
                                        })
                                    })
                                })
                            }, cb)
                        countRemaining -= (batchCount = result.length)
                    })
                },
                () => { return batchCount > 0 && countRemaining > 0 },
                cb)
        },
        updateRecord: (entity, keyName, record, cb) => {
            var old
            async.waterfall([
                cb => { self.data.get(self.namespace, entity + "/" + record[keyName], (snapshot) => {
                    old = snapshot.val()
                    cb(old && old.update_date == record.update_date && entity != "households" ? "skip-update" : null)
                }) },
                cb => { self.getRelationships(entity, record, cb) },
                cb => { self.data.update(self.namespace, entity + "/" + record[keyName], record, old, cb) }
            ], err => { cb(err == "skip-update" ? null : err) })
        }
}