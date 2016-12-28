var f = function() {
    this.list.forEach(function(content){
        emit(content,{count: 1});
    });
};

var f1 =function() {
    this.list.forEach(function(i) {
        this.list.forEach(function(j) {
            emit({pair: [i,j]},{count: 1});
        });
    });
};