$(function() {
    console.log(data)
    
    let x_axis = [];
    let y_axis_deaths = [];
    let y_axis_deaths_mavgt = [];
    let y_axis_newCases = [];
    let y_axis_newCases_mavg = [];
    
    for (let i = 0; i < data.deaths.length; i++)
    {
        x_axis.push(i);
        y_axis_deaths.push((data.deaths[i]/1000));
        if (data.deaths_mavg[i] != undefined) y_axis_deaths_mavgt.push((data.deaths_mavg[i]/1000));
    }
    
    for (let i = 0; i < data.newCases.length; i++)
    {
        x_axis.push(i);
        y_axis_newCases.push(data.newCases[i]);
        if (data.newCases_mavg[i] != undefined) y_axis_newCases_mavg.push(data.newCases_mavg[i]);
    }
    
    var line_graph_data = {
        xLabel: 'Month',
        yLabel: 'Closing Price',
        points: x_axis,
        groups: [
            {
                label: 'Deaths',
                values: y_axis_deaths
            },{
                label: 'Avg(Deaths)',
                values: y_axis_deaths_mavgt
            }/*
            ,{
                label: 'New Cases',
                values: y_axis_newCases
            },
            {
                label: 'Avg(New Cases)',
                values: y_axis_newCases_mavg
            }
            */
        ]
    };
    
    $('#line-graph').graphly({ 'data': line_graph_data, 'type': 'line', 'width': 900, 'height': 400 });
});