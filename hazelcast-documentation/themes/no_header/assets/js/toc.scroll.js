(function() {
    $('a[href^=#]').on('click', function(e) {
        var href = $(this).attr('href');
        $('html, body').animate({
            scrollTop: $(href).offset().top
        },'fast');

        e.preventDefault();
    });
}).call(this);
