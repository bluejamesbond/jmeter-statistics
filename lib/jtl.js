const split = require("split");

module.exports = function (instream, outstream, done) {

  var results = {};

  function addLabel (label) {
    if (!results[label]) {
      results[label] = {
        label: label,
        errors: 0,
        count: 0,
        responsetime_total: 0,
        responsetime_max: Number.MIN_VALUE,
        responsetime_min: Number.MAX_VALUE,
        bytes_max: Number.MIN_VALUE,
        bytes_min: Number.MAX_VALUE,
        time_start: Number.MAX_VALUE,
        time_end: Number.MIN_VALUE,
        totalBytes: 0,
        duration: 0,
        elapsedTime: [],
        apdex_ok: 0,
        apdex_warn: 0,
        apdex_fail: 0
      };
    }
  }

  function parse (line) {
    var splits = line.split(",");
    return {
      timestamp: +splits[0],
      elapsed: +splits[1],
      label: splits[2],
      statusCode: +splits[3],
      bytes: +splits[8]
    };
  }

  const apdexT = 300;

  function setSatisfaction (label, responseTime) {
    if (responseTime <= apdexT) {
      return ++results[label].apdex_ok;
    }

    if (responseTime <= 2 * apdexT) {
      return ++results[label].apdex_warn;
    }

    return ++results[label].apdex_fail;
  }

  function data (line) {
    if (line && line.indexOf('timeStamp,') < 0) {
      var row = parse(line);
      addLabel(row.label);
      ++results[row.label].count;
      if(row.statusCode !== 200) {
        ++results[row.label].errors;
      }

      results[row.label].responsetime_max = Math.max(results[row.label].responsetime_max, +row.elapsed);
      results[row.label].responsetime_min = Math.min(results[row.label].responsetime_min, +row.elapsed);
      results[row.label].bytes_min = Math.min(results[row.label].bytes_min, +row.bytes);
      results[row.label].bytes_max = Math.max(results[row.label].bytes_max, +row.bytes);
      results[row.label].totalBytes += +row.bytes;
      results[row.label].responsetime_total += +row.elapsed;
      results[row.label].time_end = Math.max(results[row.label].time_end, +row.timestamp + +row.elapsed);
      results[row.label].time_start = Math.min(results[row.label].time_start, +row.timestamp);
      results[row.label].elapsedTime.push(row.elapsed);
      setSatisfaction(row.label, row.elapsed);
    }
  }

  function calculatePercentile (dataArray, percentile) {
    var divisor = (100-percentile)/100
    dataArray.sort(function(a,b) { return b - a; });
    var xPercent = parseInt(Math.ceil(dataArray.length*divisor));
    return dataArray.slice(0, xPercent).slice(-1)[0]
  }

  function summarise () {
    for (var label of Object.keys(results)) {
      results[label].responsetime_avg = +(results[label].responsetime_total / results[label].count).toFixed(2);
      results[label].bytes_avg = +(results[label].totalBytes / results[label].count).toFixed(2);
      
      results[label].duration_s = (results[label].time_end - results[label].time_start) / 1000;
      results[label].throughput = +(results[label].count / results[label].duration_s).toFixed(2);
      results[label].apdex = +((results[label].apdex_ok + results[label].apdex_warn / 2) / results[label].count).toFixed(2);
      results[label]["error %"] = +(results[label].errors / results[label].count).toFixed(2);

      results[label].responsetime_p50 = calculatePercentile(results[label].elapsedTime, 50);
      results[label].responsetime_p90 = calculatePercentile(results[label].elapsedTime, 90);
      results[label].responsetime_p99 = calculatePercentile(results[label].elapsedTime, 99);
    }
  }

  function end () {
    summarise();
    var keys = [    "label", 
                    "count", 
                    "apdex",
                    "apdex_warn",
                    "apdex_fail",
                    "apdex_ok",
                    "bytes_min", 
                    "bytes_max", 
                    "bytes_avg", 
                    "duration_s", 
                    "throughput", 
                    "responsetime_max", 
                    "responsetime_min", 
                    "responsetime_avg", 
                    "responsetime_p50", 
                    "responsetime_p90", 
                    "responsetime_p99", 
                    "error %"
                ];
    outstream.write(keys.join(",") + "\n");
    for (var key of Object.keys(results)) {
      var values = keys.map((k) => {
        return results[key][k];
      });

      outstream.write(values.join(",") + "\n");
    }
    return done();
  }

  return instream
    .pipe(split())
    .on("data", data)
    .on("end", end)
};
