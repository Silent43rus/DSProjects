const dt_input = document.querySelector('#btn_date')
const tab_input = document.querySelector('#btn_periods')
const get_report = document.querySelector('#btn_report')
const label_dtstart = document.querySelector('input[name="start_dt"]')
const label_dtstop = document.querySelector('input[name="stop_dt"]')
const label_lines = document.querySelector('select[name="select_line"]')

const urladdres = document.location.href
const xhr = new XMLHttpRequest()
let packed = []
let dtstart
let dtstop
let line
let neg_action = []
let neg_out_curvature = []

// charts

let ctx = document.querySelector("#mychart").getContext('2d');
let ctx2 = document.querySelector("#mychart2").getContext('2d');
let ctx3 = document.querySelector("#mychart3").getContext('2d');
let ctx4 = document.querySelector("#mychart4").getContext('2d');
let ctx5 = document.querySelector("#mychart5").getContext('2d');
let ctx6 = document.querySelector("#mychart6").getContext('2d');



const bubbled1Data = {
    labels: [],
    datasets: [{
      label: "График распределения отрицательных воздействий",
      data: [],
      pointBorderColor: 'rgba(255, 255, 255, 0.5)',
      pointBackgroundColor: 'green',
      pointRadius: 5,
    }]
  };
const bubbled2Data = {
    labels: [],
    datasets: [{
      label: "График распределения положительных воздействий",
      data: [],
      pointBorderColor: 'rgba(255, 255, 255, 0.5)',
      pointBackgroundColor: 'red',
      pointRadius: 5,

    }]
  };
const chartOptions1 = {
    plugins: {
        legend: {
          display: true,
            labels: {
                color: 'rgb(180, 180, 180)',
                boxWidth: 0,
                boxHeight: 0
            }
        }
    },
    interaction: {
        mode: 'index',
        interesect: false
    },
    scales: {
        x: {
            grid: {
                color: 'rgb(80, 80, 80)',
                borderDash: [4, 4]
            },
            title: {
              display: true,
              text: 'Воздействие',
              color: 'rgb(180, 180, 180)'
              
            },
            ticks: {
                color: 'rgb(180, 180, 180)'
            }
          },
          y: {
            grid: {
                color: 'rgb(80, 80, 80)',
                borderDash: [8, 4]
            },
            title: {
              display: true,
              text: 'Выходная кривизна',
              color: 'rgb(180, 180, 180)'
            },
            // min: 0,
            // max: 100,
            ticks: {
              // forces step size to be 50 units
              stepSize: 0.1,
              color: 'rgb(180, 180, 180)'
            
            }
        }
      
    }
  };

const chartOptions2 = {
    plugins: {
        legend: {
          display: true,
            labels: {
                color: 'rgb(180, 180, 180)',
                boxWidth: 0,
                boxHeight: 0
            }
        }
    },
    interaction: {
        mode: 'index',
        interesect: false
    },
    scales: {
        x: {
            grid: {
                color: 'rgb(80, 80, 80)',
                borderDash: [4, 4]
            },
            title: {
              display: true,
              text: 'Воздействие',
              color: 'rgb(180, 180, 180)'
              
            },
            ticks: {
                color: 'rgb(180, 180, 180)'
            }
          },
          y: {
            grid: {
                color: 'rgb(80, 80, 80)',
                borderDash: [8, 4]
            },
            title: {
              display: true,
              text: 'Точность',
              color: 'rgb(180, 180, 180)'
            },
            // min: 0,
            // max: 100,
            ticks: {
              // forces step size to be 50 units
              stepSize: 10,
              color: 'rgb(180, 180, 180)'
            
            }
        }
      
    }
  };
  
  const plot1Data = {
    labels: [],
    datasets: [{
      label: "График точности отрицательных воздействий",
      data: [],
      fill: true,
      borderColor: 'red',
      pointBorderColor: 'rgba(255, 255, 255, 0.5)',
      pointBackgroundColor: 'green',
      pointRadius: 2,
    }]
  };

  const plot2Data = {
    labels: [],
    datasets: [{
      label: "График точности положительных воздействий",
      data: [],
      fill: true,
      borderColor: 'red',
      pointBorderColor: 'rgba(255, 255, 255, 0.5)',
      pointBackgroundColor: 'green',
      pointRadius: 2,
    }]
  };
  
const chartOptions3 = {
    plugins: {
        legend: {
          display: true,
            labels: {
                color: 'rgb(180, 180, 180)',
                boxWidth: 0,
                boxHeight: 0
            }
        }
    },
    interaction: {
        mode: 'index',
        interesect: false
    },
    scales: {
        x: {
            grid: {
                color: 'rgb(80, 80, 80)',
                borderDash: [4, 4]
            },
            title: {
              display: true,
              text: 'Кривизна',
              color: 'rgb(180, 180, 180)'
              
            },
            ticks: {
                color: 'rgb(180, 180, 180)'
            }
          },
          y: {
            grid: {
                color: 'rgb(80, 80, 80)',
                borderDash: [8, 4]
            },
            title: {
              display: true,
              text: 'Воздействие',
              color: 'rgb(180, 180, 180)'
            },
            // min: 0,
            // max: 100,
            ticks: {
              // forces step size to be 50 units
              stepSize: 0.1,
              color: 'rgb(180, 180, 180)'
            
            }
        }
      
    }
  };

  const mix1Data = {
    labels: [],
    datasets: [{
        type: 'bubble',
        label: "Фактическая таблица отрицательных воздействий от кривизны",
      data: [],
      fill: true,
      borderColor: 'red',
      pointBorderColor: 'rgba(255, 255, 255, 0.5)',
      pointBackgroundColor: 'green',
      pointRadius: 5,
    }]
  };

  const mix2Data = {
    labels: [],
    datasets: [{
        type: 'bubble',
        label: "Фактическая таблица положительных воздействий от кривизны",
      data: [],
      fill: true,
      borderColor: 'red',
      pointBorderColor: 'rgba(255, 255, 255, 0.5)',
      pointBackgroundColor: 'red',
      pointRadius: 5,
    }]
  };

let mych = new Chart(ctx, {
    type: 'bubble',
    data: bubbled1Data,
    options: chartOptions1
} )
let mych2 = new Chart(ctx2, {
    type: 'bubble',
    data: bubbled2Data,
    options: chartOptions1
} )
let mych3 = new Chart(ctx3, {
    type: 'line',
    data: plot1Data,
    options: chartOptions2
} )
let mych4 = new Chart(ctx4, {
    type: 'line',
    data: plot2Data,
    options: chartOptions2
} )
let mych5 = new Chart(ctx5, {
    data: mix1Data,
    options: chartOptions3
} )
let mych6 = new Chart(ctx6, {
    data: mix2Data,
    options: chartOptions3
} )


function updateChart2(chart, x, y) {

  chart.data.datasets[0].data = y
  chart.data.labels = x
  chart.update()

}

function updateChart1(chart, x, y, z) {

  colors = []
  chart.data.datasets[0].data = y
  chart.data.labels = x
  z.forEach(function(item, index){
    if (item == 'True'){

      colors.push('green')

    }
    else{
      colors.push('red') 
    }
  })
  chart.data.datasets[0].pointBackgroundColor = colors
  chart.update()

}


function openInput()
{
    document.querySelector('#output_form').classList.toggle('active_menu')
    document.querySelector('div[onclick="openInput()"]').classList.toggle('active_menu')
    
}

function postRequest(method, url, body = null){
    return new Promise((resolve, reject) => {
        const xhr = new XMLHttpRequest()
        const csrfToken = document.querySelector('#csrf-token').content
  
        xhr.open(method, url, true)
        xhr.responseType ='json'
        xhr.setRequestHeader('X-CSRFToken', csrfToken)
        xhr.setRequestHeader('Content-Type', 'application/json; charset=utf-8')
        xhr.setRequestHeader('Accept', 'application/json')
    
        xhr.onload = () => {
             xhr.status >= 400 ? reject(xhr.response) : resolve(xhr.response)
        }
  
        xhr.onerror = () => {
            reject(xhr.response)
        }
  
        xhr.send(JSON.stringify(body))
        
    })
  }

// Выбор периода, формирование таблиц
dt_input.addEventListener('click', function(e){
    dtstart = label_dtstart.value
    dtstop = label_dtstop.value
    line = label_lines.value
    if (dtstart == '' || dtstop == '' || line == ''){
      alert('Введите начальную и конечную дату')
    }
    else{
      const select_period = document.querySelector('select[name="select_period"]')
      
      body = {
        start_dt: dtstart,
        stop_dt: dtstop,
        line_num: line
      }
      a = postRequest("POST", urladdres, body)
      a.then(data => {
        while(document.querySelector("#periods") != null){
          document.querySelector('#periods').remove()
        }
        for (strr of data['tables']){
          let elem = document.createElement('option')
          elem.value = strr
          elem.text = strr
          elem.id = 'periods'
          select_period.append(elem)
        }
      })
    }
  
  })

// Выбор таблицы, формирование пачек
tab_input.addEventListener('click', function(e)
{
    const tab_input = document.querySelector('select[name="select_period"]')
    if (tab_input.value == ''){
        alert("Выберите период")
    }
    else
    {
        const nom_input = document.querySelector('select[name="select_nomen"]')
        a = postRequest('POST', urladdres, {
            select_period: tab_input.value,
            line_num: label_lines.value
        })
        a.then(data => {
            while(document.querySelector('#nomens') != null)
            {
                document.querySelector('#nomens').remove()
            }
            packed = data['packed']
            start_dt = data['period'][0]
            stop_dt = data['period'][1]
            data['nomen'].forEach(function(item, index){
                let elem = document.createElement('option')
                elem.value = index
                elem.text = item
                elem.id = 'nomens'
                nom_input.append(elem)
                
            })
        })
    }
})

// Выбор номенклатуры, формирование отчета
get_report.addEventListener('click', function(e)
{
    const nom_input = document.querySelector('select[name="select_nomen"]')
    if (nom_input.value == '')
    {
        alert("Выберите номенклатуру")
    }
    else{
        // console.log(nom_input.value)
        // console.log(packed[nom_input.value])
        // console.log(start_dt, stop_dt)
        a = postRequest('POST', urladdres, {
            select_packed : packed[nom_input.value],
            start_table : start_dt,
            stop_table : stop_dt,
            line_num: label_lines.value
        })
        a.then(data => {
          tab_data = [data['count_bar'], data['count_actions'], data['accuracy'],
                      data['pos_overfill'], data['pos_underfill'], data['pos_accuracy'],
                      data['neg_overfill'], data['neg_underfill'], data['neg_accuracy'], 
                      data['nomen'], data['dt_start'], data['dt_stop']
                    ]
          document.querySelector('td[name="count_bar"]').textContent = tab_data[0]
          document.querySelector('td[name="count_actions"]').textContent = tab_data[1]
          document.querySelector('td[name="accuracy"]').textContent = tab_data[2]
          document.querySelector('td[name="pos_overfill"]').textContent = tab_data[3]
          document.querySelector('td[name="pos_underfill"]').textContent = tab_data[4]
          document.querySelector('td[name="pos_accuracy"]').textContent = tab_data[5]
          document.querySelector('td[name="neg_overfill"]').textContent = tab_data[6]
          document.querySelector('td[name="neg_underfill"]').textContent = tab_data[7]
          document.querySelector('td[name="neg_accuracy"]').textContent = tab_data[8]

          document.querySelector('p[name="text-nomen"]').textContent = `Номенклатура: ${tab_data[9]}`
          document.querySelector('p[name="text-periods"]').textContent = `Период: ${tab_data[10]} - ${tab_data[11]}`

          
          // scatters
          const str = data['neg_action'].toString(), neg_action = str.match(/-?\d+(?:\.\d+)?/g).map(Number)
          const str1 = data['neg_out_curvature'].toString(), neg_out_curvature = str1.match(/-?\d+(?:\.\d+)?/g).map(Number)
          const neg_firs_correct = data['neg_firs_correct']
          
          const str2 = data['pos_action'].toString(), pos_action = str2.match(/-?\d+(?:\.\d+)?/g).map(Number)
          const str3 = data['pos_out_curvature'].toString(), pos_out_curvature = str3.match(/-?\d+(?:\.\d+)?/g).map(Number)
          const pos_firs_correct = data['pos_firs_correct']

          const str4 = data['neg_x'].toString(), neg_x = str4.match(/-?\d+(?:\.\d+)?/g).map(Number)
          const str5 = data['neg_y'].toString(), neg_y = str5.match(/-?\d+(?:\.\d+)?/g).map(Number)
          
          const str6 = data['pos_x'].toString(), pos_x = str6.match(/-?\d+(?:\.\d+)?/g).map(Number)
          const str7 = data['pos_y'].toString(), pos_y = str7.match(/-?\d+(?:\.\d+)?/g).map(Number)

          const str8 = data['neg_l'].toString(), neg_l = str8.match(/-?\d+(?:\.\d+)?/g).map(Number)
          const str9 = data['pos_l'].toString(), pos_l = str9.match(/-?\d+(?:\.\d+)?/g).map(Number)

          const str10 = data['pos_curvature'].toString(), pos_curvature = str10.match(/-?\d+(?:\.\d+)?/g).map(Number)
          const str11 = data['neg_curvature'].toString(), neg_curvature = str11.match(/-?\d+(?:\.\d+)?/g).map(Number)

          updateChart1(mych, neg_action, neg_out_curvature, neg_firs_correct)
          updateChart1(mych2, pos_action, pos_out_curvature, pos_firs_correct)
          updateChart2(mych3, neg_x, neg_y)
          updateChart2(mych4, pos_x, pos_y)
          updateChart1(mych5, neg_curvature, neg_action, neg_firs_correct)
          updateChart1(mych6, pos_curvature, pos_action, pos_firs_correct)

        })
    }
})

