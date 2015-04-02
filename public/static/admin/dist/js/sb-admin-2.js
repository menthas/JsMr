var current_section = "dashboard";

//Loads the correct sidebar on window load,
//collapses the sidebar on window resize.
// Sets the min-height of .page-wrapper to window size
$(function() {
    $('.alert').hide();
    $('#side-menu').metisMenu();
    $(window).bind("load resize", function() {
        topOffset = 50;
        width = (this.window.innerWidth > 0) ? this.window.innerWidth : this.screen.width;
        if (width < 768) {
            $('div.navbar-collapse').addClass('collapse');
            topOffset = 100; // 2-row-menu
        } else {
            $('div.navbar-collapse').removeClass('collapse');
        }

        height = ((this.window.innerHeight > 0) ? this.window.innerHeight : this.screen.height) - 1;
        height = height - topOffset;
        if (height < 1) height = 1;
        if (height > topOffset) {
            $(".page-wrapper").css("min-height", (height) + "px");
        }
    });

    var url = window.location;
    var element = $('ul.nav a').filter(function() {
        return this.href == url || url.href.indexOf(this.href) == 0;
    }).addClass('active').parent().parent().addClass('in').parent();
    if (element.is('li')) {
        element.addClass('active');
    }

    $(".main-nav").click(function () {
        var section = $(this).data("section");
        if (current_section == section)
            return false;
        $('#page-' + current_section).slideUp();
        load_page(section);
    });

    // load dashboard and reload content every 5 seconds
    load_page(current_section);
    setInterval(function () { load_page(current_section) }, 5000);
});

function load_page(section) {
    if (section == "dashboard")
        load_dashboard();
    else if (section == "clients")
        load_clients();
    else
        load_jobs();
}

function load_dashboard() {
    $.get('/admin/dashboard', {}, function (result) {
        if (result.error) {
            $('#dashboard-alert').fadeIn();
        } else {
            $('#dashboard-client-count').html(result.clients);
            $('#dashboard-job-count').html(result.jobs);
        }
        $('#page-dashboard').slideDown();
    }, 'json');
}
