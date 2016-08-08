const split = require("split");

module.exports = function (instream, outstream, done) {

  var results = {};

  function addLabel (label) {
    if (!results[label]) {
      results[label] = {
        label: label,
        errors: 0,
        samples: 0,
        max: Number.MIN_VALUE,
        min: Number.MAX_VALUE,
        duration: 0,
        elapsedTime: [],
        satisfied: 0,
        tolerating: 0,
        frustrated: 0
      };
    }
  }

  function parse (line) {
    var splits = line.split(",");
    return {
      timestamp: +splits[0],
      elapsed: +splits[1],
      label: splits[2],
      statusCode: +splits[3]
    };
  }

  const apdexT = 300;

  function setSatisfaction (label, responseTime) {
    if (responseTime <= apdexT) {
      return ++results[label].satisfied;
    }

    if (responseTime <= 2 * apdexT) {
      return ++results[label].tolerating;
    }

    return ++results[label].frustrated;
  }

  function data (line) {
    if (line && line.indexOf('timeStamp,') < 0) {
      var row = parse(line);
      addLabel(row.label);
      ++results[row.label].samples;
      if(row.statusCode !== 200) {
        ++results[row.label].errors;
      }

      results[row.label].max = Math.max(results[row.label].max, +row.elapsed);
      results[row.label].min = Math.min(results[row.label].min, +row.elapsed);
      results[row.label].duration += +row.elapsed;
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
      results[label].average = +(results[label].duration / results[label].samples).toFixed(2);

      results[label]["throughput (rps)"] = +((1000 * results[label].samples) / results[label].duration).toFixed(2);
      results[label].apdex = +((results[label].satisfied + results[label].tolerating / 2) / results[label].samples).toFixed(2);
      results[label]["% error"] = +(results[label].errors / results[label].samples).toFixed(2);

      results[label]["%50 line"] = calculatePercentile(results[label].elapsedTime, 50);
      results[label]["%90 line"] = calculatePercentile(results[label].elapsedTime, 90);
      results[label]["%99 line"] = calculatePercentile(results[label].elapsedTime, 99);
    }
  }

  function end () {
    summarise();
    var keys = ["label", "samples", "duration", "throughput (rps)", "average", "%50 line", "%90 line", "%99 line", "% error", "apdex", "satisfied", "tolerating", "frustrated"];
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
