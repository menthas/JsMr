var current_section = "dashboard";
var dashboard, clients, jobs;

//Loads the correct sidebar on window load,
//collapses the sidebar on window resize.
// Sets the min-height of .page-wrapper to window size
$(function() {
    dashboard = $('#page-dashboard');
    clients = $('#page-clients');
    jobs = $('#page-jobs');
    clients.hide().data("loaded", false);
    jobs.hide().data("loaded", false);
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

    $(".main-nav").click(function () {
        var section = $(this).data("section");
        if (current_section == section)
            return false;
        $('#page-' + current_section).slideUp();
        load_page(section);
        current_section = section;
    });

    // load dashboard and reload content every 5 seconds
    load_page(current_section);
    setInterval(function () { load_page(current_section); }, 20000);
    $('#reload-btn').click(function() { load_page(current_section); });
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
            var server_from = new Date();
            server_from.setSeconds(server_from.getSeconds() - result.uptime);
            var avg_time = Math.round(result.avg_uptime * 100) / 100;
            if (avg_time > 10)
                avg_time = Math.round(avg_time);
            $('#dashboard-client-count').html(result.clients);
            $('#dashboard-job-count').html(result.jobs);
            $('#dashboard-avg-time').html(format_time(avg_time));
            $('#dashboard-total-clients').html(result.total_clients);
            $('#dashboard-uptime').html(format_time(result.uptime));
            $('#dashboard-server-from').html(server_from.toDateString());
        }
        dashboard.slideDown();
    }, 'json');
}

function load_clients() {
    if (!clients.data("loaded")) {
        var table = $('#clients-table').dataTable({
            serverSide: true,
            ajax: "/admin/clients",
            ordering: false,
        });
        clients.data('loaded', true);
        clients.data('table', table);
    } else {
        clients.data('table').api().ajax.reload(null, false);
    }
    clients.slideDown();
}

function load_jobs() {
    if (!jobs.data("loaded")) {
        var table = $('#jobs-table').dataTable({
            serverSide: true,
            ajax: "/admin/jobs",
            ordering: false,
        });
        jobs.data('loaded', true);
        jobs.data('table', table);
    } else {
        jobs.data('table').api().ajax.reload(null, false);
    }
    jobs.slideDown();
}

function format_time(seconds) {
    var f = "";
    if (seconds > 3600) {
        var hours = Math.floor(seconds / 3600);
        seconds -= hours * 3600;
        f += hours + "h ";
    }
    if (seconds > 60) {
        var minutes = Math.floor(seconds / 60);
        seconds -= minutes * 60;
        f += minutes + "m ";
    }
    return f + seconds + "s";
}
