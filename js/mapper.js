var f = function() {
    for (var i = 0; i < this.content.length; i++) {
        emit(this.content[i],1);
    }
};