var refreshRate = 10000;
var apiUrl = "";

$reportCreateResult = $('#tpsResult > ul');
$reportList = $('#reportServiceTable > ul');

$('#processTpsReport').click(function () {
    var value = $('#processReportName').val();
    if (value) {
        processTpsReport(apiUrl, value, $reportList);
    }
});

$('#createTpsReport').click(function () {
    var value = $('#newReportName').val();
    if (value) {
        createTpsReport(apiUrl, value, $reportCreateResult);
    }
});

startRefresh = function () {
    refreshReportList(apiUrl, $reportList);
};

setApiUrl = function (url)
{
    apiUrl = url;
}

createTpsReport = function (apiUrl, name, $resultElement) {
    var start = new Date().getTime();
    var requestUrl = apiUrl + '/api/tps/' + name;
    
    $.ajax({
        url: requestUrl,
        method: 'POST',
        contentType: 'application/json',
        dataType: 'json'
    })
        .done(function (data, textStatus, jqXHR) {
            $resultElement.append(
                '<li><label>Name </label>' + data.name + '</li>' +
                '<li><label>ID </label>' + data.id + '</li>' +
                '<li><label>Link</label><a href="#">Download</a>'
            );

            end = new Date().getTime();
            updateFooter(jqXHR, 'POST', requestUrl, end - start);

        })
        .fail(function (jqXHR, textStatus, errorThrown) {
            end = new Date().getTime();
            updateFooter(jqXHR, 'POST', requestUrl, end - start);
        })
        .always(function () {
        });
}

processTpsReport = function (apiUrl, name, $reportList) {
    var start = new Date().getTime();
    var requestUrl = apiUrl + '/api/reports/' + name;
    
    $.ajax({
        url: requestUrl,
        method: 'POST',
        contentType: 'application/json',
        dataType: 'json'
    })
        .done(function (data, textStatus, jqXHR) {
            end = new Date().getTime();
            updateFooter(jqXHR, 'POST', requestUrl, end - start);
        })
        .fail(function (jqXHR, textStatus, errorThrown) {
            end = new Date().getTime();
            updateFooter(jqXHR, 'POST', requestUrl, end - start);
        })
        .always(function () {
        });
}


deleteReportService = function (apiUrl, name) {
    var start = new Date().getTime();
    var requestUrl = apiUrl + '/api/reports/' + name;
    
    $.ajax({
        url: requestUrl,
        method: 'DELETE',
        contentType: 'application/json',
        dataType: 'json'
    })
        .done(function (data, textStatus, jqXHR) {
            end = new Date().getTime();
            updateFooter(jqXHR, 'DELETE', requestUrl, end - start);
        })
        .fail(function (jqXHR, textStatus, errorThrown) {
            end = new Date().getTime();
            updateFooter(jqXHR, 'DELETE', requestUrl, end - start);
        })
        .always(function () {
            $(this).prop('disabled', true).val('Delete');
        });
}

refreshReportList = function (apiUrl, $reportList) {
    var start = new Date().getTime();
    var requestUrl = apiUrl + '/api/reports';

    $.ajax({
        url: requestUrl,
        method: 'GET',
        contentType: 'application/json',
        dataType: 'json'
    })
        .done(function (data, textStatus, jqXHR) {

            $reportList.html('');
            for (var i = 0; i < data.length; ++i) {
                var reportService = data[i];
                var reportName = reportService.name.substr(reportService.name.lastIndexOf('/') + 1);

                var $reportUl = $(
                    '<li>' +
                    '<div class="panel panel-default reportServiceListItem">' +
                    '<h3>' + reportName + '</h3>' +
                    '<ul>' +
                    '<li><label>Service:</label>' + reportService.name + '</li > ' +
                    '<li><label>Status:</label>' + reportService.status + '</li > ' +
                    '<li><label>Health:</label>' + reportService.health + '</li > ' +
                    '</ul>' +
                    '<button class="btn btn-danger" onclick="deleteReportService(\'' + apiUrl + '\', \'' + reportName + '\')" type="button">Delete</button>' +
                    '<button class="btn btn-primary" onclick="refreshReportStatus(\'' + apiUrl + '\', \'' + reportName + '\')" style="margin-left:20px" type="button">Get Status</button>' +
                    '</div > ' +
                    '</li>');

                $reportList.append($reportUl);
            }

            end = new Date().getTime();
            updateFooter(jqXHR, 'GET', requestUrl, end - start);
        })
        .fail(function (jqXHR, textStatus, errorThrown) {

            end = new Date().getTime();
            updateFooter(jqXHR, 'GET', requestUrl, end - start);
        })
        .always(function (data, status, http) {
            setTimeout(function () {
                refreshReportList(apiUrl, $reportList);
            }, refreshRate);
        });
}

refreshReportStatus = function (apiUrl, reportName) {
    var start = new Date().getTime();
    var requestUrl = apiUrl + '/api/reports/' + reportName + '/status';

    $.ajax({
        url: requestUrl,
        method: 'GET',
        contentType: 'application/json',
        dataType: 'json'
    })
        .done(function (data, textStatus, jqXHR) {
            $('#reportServiceDetails').html(
                '<div class="panel panel-primary reportDetails">' +
                '<h3>' + reportName + '</h3>' +
                '<ul>' +
                '<li><label>Report Status</label>' + data.status + '</li>' +
                '<li><label>Steps remaining</label>' + data.remaining + '</li>' +
                '</ul>' +
                '</div>');

            end = new Date().getTime();
            updateFooter(jqXHR, 'GET', requestUrl, end - start);
        })
        .fail(function (jqXHR, textStatus, errorThrown) {

            end = new Date().getTime();
            updateFooter(jqXHR, 'GET', requestUrl, end - start);
        })
}


/*This function hides the footer*/
function toggleFooter(option) {
    var footer = $('#footer');
    switch (option) {
        case 0:
            footer.addClass('hidden');
            break;
        case 1:
            footer.removeClass('hidden');
            break;
    }
}

/*This function puts HTTP result in the footer */
function updateFooter(jqXHR, method, requestUrl, timeTaken) {
    toggleFooter(1);
    if (jqXHR.status < 400) {
        statusPanel.className = 'panel resultList panel-success';
        statusPanelHeading.innerHTML = jqXHR.status + ' ' + jqXHR.statusText;
        statusPanelBody.innerHTML = '<label>HTTP/1.1 ' + method + '</label> ' + requestUrl + ' (' + timeTaken.toString() + ' ms)';
    }
    else {
        statusPanel.className = 'panel resultList panel-danger';
        statusPanelHeading.innerHTML = jqXHR.status + ' ' + jqXHR.statusText;
        statusPanelBody.innerHTML = jqXHR.responseText;
    }
}