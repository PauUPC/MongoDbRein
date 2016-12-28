var f = function(key,values) {
    var total = 0;
    for (var i = 0; i < values.length; i++) {
        total += values[i];
    }
    return total;
};

var f1 = function(key,values) {
    var sum = 0;
    values.forEach(function(values) {
        sum += values['count'];
    });
    return {name: key, count: sum};
};