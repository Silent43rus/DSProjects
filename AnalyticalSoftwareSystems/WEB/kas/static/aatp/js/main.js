// Обработчик для страниц ААТП 3 клети, ААТП резки, ААТП 45 клети

// Начальные параметры
let req_prof = document.querySelector('#btn_date')
let row = document.getElementsByTagName('tr');
let gen_rap = document.querySelector('#btn_generate_raport')
const urladdres = document.location.href
let mycharts 
const xhr = new XMLHttpRequest()
let ref_profiles
let profiles
let find_data = {
    ref_profile: [],
    description_profile : [],
    dt_start : [],
    dt_stop : [],
    dt: []
}
Chart.defaults.font.size = 18
let id_profile
let lab_location
if (document.location.pathname == '/aatp_rezka'){
    lab_location = 4
}
if (document.location.pathname == '/aatp3'){
    lab_location = 2
}
if (document.location.pathname == '/aatp_45'){
    lab_location = 3
}
// Запрос на список всех профилей
load_profiles()

// Функция для передачи пост запросов серверу
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


// Функция рисования линий с цветом в соответсвии нахождением значения за границами или нет (сейчас не используется)
function createDataset(x, y, col, l){
    let dataFrame = []
    let rNull = []
    let lNull = []
    for (let i = 0; i < x.length - 1; i++){
        rNull.push('null')
    }
    dataFrame.push({
        label: `${l + 1} Проход`,
        data: y.slice(0, 2).concat(rNull),
        fill: true,
        borderColor: col[1],
        pointBorderColor: 'rgba(255, 255, 255, 0.5)',
        pointRadius: 2
    })
    rNull.pop('null')
    lNull.push('null')
    for (let i = 1; i < x.length - 1; i++){
        dataFrame.push({
            data: lNull.concat(y.slice(i, i + 2)).concat(rNull),
            fill: true,
            borderColor: col[i + 1],
            pointBorderColor: 'rgba(255, 255, 255, 0.5)',
            pointRadius: 2
        })
        rNull.pop('null')
        lNull.push('null')
    }

    return dataFrame
}



// Функция создания графиков по проходам 
function createChart(chart, x, y, col, rupar, tekpar, i, qurt){
    let label 
    if (lab_location == 2 || lab_location == 3){
        label = 'прохода'
    }
    if (lab_location == 4){
        label = 'реза'
    }
    const horizontalArbitraryLine = {
        id: 'horizontalArbitraryLine',
        beforeDraw(chart, args, options){
            const {ctx, chartArea: {top, right, bottom, left, width, height}, scales:
                {x, y} } = chart;
            ctx.save();
            const alpha = options.alpha
            const q1 = options.q1 //< top && options.q1 > bottom ? options.q1 : options.q1 > top ? top : bottom
            const q3 = options.q3 //< top && options.q3 > bottom ? options.q3 : options.q3 > top ? top : bottom
            const x1 = options.x1 //< top && options.x1 > bottom ? options.x1 : options.x1 > top ? top : bottom
            const x2 = options.x2 //< top && options.x2 > bottom ? options.x2 : options.x2 > top ? top : bottom
            let gr = options.gr
            let yel = options.yel
            let rd = options.rd

            // Зеленая зона
            if (gr){
            ctx.fillStyle = `rgba(0, 200, 0, ${alpha})`;
            ctx.fillRect(left, y.getPixelForValue(q3), width, y.getPixelForValue(q1) - y.getPixelForValue(q3));
            ctx.restore();
            }

            // Желтая зона
            if (yel){
            ctx.strokeStyle = 'yellow';
            ctx.strokeRect(left, y.getPixelForValue(q3), width, 0);
            ctx.restore();

            ctx.strokeStyle = 'yellow';
            ctx.strokeRect(left, y.getPixelForValue(q1), width, 0);
            ctx.restore();

            ctx.fillStyle = `rgba(252, 241, 0, ${alpha})`;
            ctx.fillRect(left, y.getPixelForValue(x2), width, y.getPixelForValue(q3) - y.getPixelForValue(x2));
            ctx.restore();

            ctx.fillStyle = `rgba(252, 241, 0, ${alpha})`;
            ctx.fillRect(left, y.getPixelForValue(q1), width, y.getPixelForValue(x1) - y.getPixelForValue(q1));
            ctx.restore();
            }

            if (rd){
            // Красная зона
            ctx.strokeStyle = 'red';
            ctx.strokeRect(left, y.getPixelForValue(x2), width, 0);
            ctx.restore();

            ctx.strokeStyle = 'red';
            ctx.strokeRect(left, y.getPixelForValue(x1), width, 0);
            ctx.restore();

            ctx.fillStyle = `rgba(239, 21, 32, ${alpha})`;
            ctx.fillRect(left, top, width, y.getPixelForValue(x2) - top);
            ctx.restore();

            ctx.fillStyle = `rgba(239, 21, 32, ${alpha})`;
            ctx.fillRect(left, y.getPixelForValue(x1), width, bottom - y.getPixelForValue(x1));
            ctx.restore();
            }
        }
    };
    const plotData = {
        labels: x,
        datasets: [{
            label: `${rupar[tekpar]} ${i + 1} ${label}`,
            data: y,
            // fill: true,
            borderColor: '#f3851f',
            backgroundColor: '#f3851f',
            pointRadius: 1, 
            borderWidth: 2
        }]
      };
      const chartOptions = {
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
        plugins: {
            horizontalArbitraryLine: {
                q1: qurt['q1'],
                q3: qurt['q3'],
                x1: qurt['whislo'],
                x2: qurt['whishi'],
                alpha: 0.25,
                gr: true,
                yel: true,
                rd: true
            }

        },
        scales: {
            x: {
                type: 'time',
                time: {
                    parser: false,
                    tooltipFormat: 'HH:mm',
                    displayFormats: {
                        millisecond: 'HH:mm:ss.SSS',
                        second: 'HH:mm:ss',
                        minute: 'HH:mm',
                        hour: 'HH'

                    }
                },
                grid: {
                    color: 'rgb(80, 80, 80)',
                    borderDash: [4, 4]
                },
                title: {
                  display: true,
                  text: 'Время',
                  color: 'rgb(180, 180, 180)',
                  
                  
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
                  text: rupar[tekpar],
                  color: 'rgb(180, 180, 180)'
                },
                min: qurt['whislo']*(0.5),
                max: qurt['whishi']*1.3,
                ticks: {
                  // forces step size to be 50 units
                  stepSize: 0.1,
                  color: 'rgb(180, 180, 180)'
                
                }
            }
          
        }
      };

    let ctx = chart.getContext('2d');
    
    mycharts.push(new Chart(ctx, {
        type: 'line',
        data: plotData,
        options: chartOptions,
        plugins: [horizontalArbitraryLine]
    } )
    )
}

// Свернуть развернуть меню ввода данных
function openInput()
{
    document.querySelector('#output_form').classList.toggle('active_menu')
    document.querySelector('div[onclick="openInput()"]').classList.toggle('active_menu')
    
}

// Делать отбор данных по профилю
function onProfile()
{
    document.querySelector('input[name="choice_dt"]').disabled = true
    document.querySelector('select[name="choice_profile"]').disabled = false
}

// Делать отбор данных по дате
function onDate()
{
    document.querySelector('input[name="choice_dt"]').disabled = false
    document.querySelector('select[name="choice_profile"]').disabled = true
}

// Обновление таблицы с профилями и их периодами проката
function writeProfiles(a, tbody){
    a.then(data => {
        while(document.querySelector('tr[id*="profile_dat"]') != null){
            document.querySelector('tr[id*="profile_dat"]').remove()
        }
        for (let i = 0; i < data['description_profile'].length; i++){
            let elem = document.createElement('tr')
            elem.id = `profile_data_${i}`
            let nom = document.createElement('td')
            let start = document.createElement('td')
            let stop = document.createElement('td')
            nom.textContent = data['description_profile'][i]
            nom.value = data['description_profile'][i]
            start.textContent = data['dt_start'][i]
            start.value = data['dt_start'][i]
            stop.textContent = data['dt_stop'][i]
            stop.value = data['dt_stop'][i]
            elem.append(nom, start, stop)
            tbody.append(elem)
        }
        find_data['ref_profile'] = data['ref_profile']
        find_data['description_profile'] = data['description_profile']
        find_data['dt_start'] = data['dt_start']
        find_data['dt_stop'] = data['dt_stop']
        find_data['dt'] = data['dt']

        row = document.getElementsByTagName('tr');
        console.log(row)
        clicker()
    })
}

// Формирование запроса на список проката профилей и обработка ответа
req_prof.addEventListener('click', function(e) {
    const choice_dt = document.querySelector('input[name="choice_dt"]')
    const choice_profile = document.querySelector('select[name="choice_profile"]')
    if (choice_profile.disabled == true && choice_dt.disabled == true)
    {
        alert("Выберите параметр отбора")
    }
    else{
        const tbody = document.querySelector('tbody')
        // Если выбрали отбор по профилю
        if (choice_profile.disabled == false){
            a = postRequest('POST', urladdres, {
                profile_choice: 'profile',
                data: ref_profiles[profiles.findIndex(i => i == choice_profile.value)]
            })
            writeProfiles(a, tbody)
        }
        //Если выбрали отбор по дате
        else{
            if (choice_dt.value == ''){
                alert("Выберите дату")
            }
            else{
                a = postRequest('POST', urladdres, {
                    profile_choice: 'date',
                    data: choice_dt.value
                })
                writeProfiles(a, tbody)
                
            }
        }
    }
})


// Стартовая функция загрузки всех профилей
function load_profiles(){
    const choice_profile = document.querySelector('select[name="choice_profile"]')
    let label
    let label1
    if (lab_location == 2 || lab_location == 3){
        label = 'проходов'
        label1 = 'Прохода'
    }
    if (lab_location == 4){
        label = 'резов'
        label1 = 'Реза'
    }
    document.querySelector('th[name="start"]').textContent = `Начало ${label}`
    document.querySelector('th[name="end"]').textContent = `Окончание ${label}`
    document.querySelector('th[name="num_action"]').textContent = `№ ${label1}`
    a = postRequest('POST', urladdres, {
        init_param: '1'
    })
    a.then(data => {
        while(document.querySelector("#profile") != null){
          document.querySelector('#profile').remove()
        }
        ref_profiles = data['ref_profiles']
        profiles = data['profiles']
        for (strr of data['profiles']){
          let elem = document.createElement('option')
          elem.value = strr
          elem.text = strr
          elem.id = 'profile'
          choice_profile.append(elem)
        }
      })   
}


// Функция выбора профиля из таблицы с профилей
function clicker(){
[].forEach.call(row, function(elem){
    elem.addEventListener('click', function (el) { 
        // alert(this.children[0].innerHTML);  
        if (this.children[0].innerHTML != 'Номенклатура'){
            if (document.querySelector('.choice') != null) {
                document.querySelector('.choice').classList.remove('choice')
            }
            this.classList.add('choice')
            id_profile = this.id.split('_')[2]
            // alert(find_data['dt_start'][this.id.split('_')[2]])
        }
    })
});
}


// Функция рисования уставок в виде коробчатых диаграм
function createChartBox(chart, quar, rupar, tekpar){
    let label
    if (lab_location == 2 || lab_location == 3){
        label = 'прохода'
    }
    if (lab_location === 4){
        label = 'реза'
    }
    console.log(quar.length)
    dat = []
    lab = []
    for (i = 0; i < quar.length; i++){
        dat.push({
            min: quar[i]['whislo'],
            q1: quar[i]['q1'],
            median: quar[i]['med'],
            q3: quar[i]['q3'],
            max: quar[i]['whishi']
        })
        lab.push((i+1).toString())
    }
    
    const plotData = {
        labels: lab,
        datasets: [{
            label: 'Распределение параметров',
            outlierColor: '#999999',
            padding: 0,
            itemRadius: 0,
            data: dat,
            backgroundColor: 'rgba(245, 136, 33, 0.5',
            borderColor: '#f3851f',
            borderWidth: 1
            // pointBorderColor: 'rgba(255, 255, 255, 0.5)',
            // pointRadius: 2
        }]
      };
      const chartOptions = {
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
                  text: `Номер ${label}`,
                  color: 'rgb(180, 180, 180)',
                  
                  
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
                  text: rupar[tekpar],
                  color: 'rgb(180, 180, 180)'
                },
                // min: qurt['whislo']*(0.5),
                // max: qurt['whishi']*1.3,
                ticks: {
                  // forces step size to be 50 units
                  stepSize: 0.1,
                  color: 'rgb(180, 180, 180)'
                
                }
            }
          
        }
      };


    let ctx = chart.getContext('2d');
    let mych = new Chart(ctx, {
        type: 'boxplot',
        data: plotData,
        options: chartOptions
    } )
}

// Формирование запроса на получение отчета и обработка ответа (построение графиков)
gen_rap.addEventListener('click', function(e) {
    const tekparam = document.querySelector('select[name="choice_param"]').value
    const starttime = document.querySelector('input[name="choice_m_start"]')
    const stoptime = document.querySelector('input[name="choice_m_stop"]')
    if (id_profile == null){
        alert('Выберите профиль из таблицы')
    }
    else{
        a = postRequest('POST', urladdres, {
            raport_par: '1',
            tekparam: tekparam,
            refnomen: find_data['ref_profile'][id_profile],
            startperiod: find_data['dt_start'][id_profile],
            stopperiod: find_data['dt_stop'][id_profile],
            tekdata: find_data['dt'][id_profile],
            starttime: starttime.value,
            endtime: stoptime.value
        })
        a.then(data => {
            const charts = document.querySelector('#charts')
            const ust_ch = document.querySelector('#ust_ch')
            while(document.querySelector(".chart.left.p") != null){
                document.querySelector('.chart.left.p').remove()
              }
            mycharts = []
            for (let i = 0; i < data['xdata'].length; i++){
                let elem = document.createElement('div')
                elem.classList.add('chart')
                elem.classList.add('left')
                elem.classList.add('p')
                elem.id = i
                let chart = document.createElement('canvas')
                
                createChart(chart, data['xdata'][i], data['ydata'][i], data['listcol'][i], data['ruparam'], data['tekparam'], i, data['val_stats'][i], data['location'])
                chart.id = `mychart_${i}`
                elem.append(chart)
                charts.append(elem)
            }
            document.querySelector('p[name="text-periods"]').textContent = `Период: ${find_data['dt_start'][id_profile].slice(0, 16)} - ${find_data['dt_stop'][id_profile].slice(0, 16)}`
            document.querySelector('p[name="text-nomen"]').textContent = `Номенклатура: ${find_data['description_profile'][id_profile]}`

            const ust_table = document.querySelector('tbody[name="ustavki-body"]')
            while(document.querySelector('#ust') != null){
                document.querySelector('#ust').remove()
              }
            for (i = 0; i < data['val_stats'].length; i++){
                let elem = document.createElement('tr')
                elem.id = 'ust'
                let num = document.createElement('td')
                let q1 = document.createElement('td')
                let q3 = document.createElement('td')
                let x1 = document.createElement('td')
                let x2 = document.createElement('td')
                num.textContent = i + 1
                q1.textContent = data['val_stats'][i]['q1']
                q3.textContent = data['val_stats'][i]['q3']
                x1.textContent = data['val_stats'][i]['whislo']
                x2.textContent = data['val_stats'][i]['whishi']
                elem.append(num)
                elem.append(q1)
                elem.append(q3)
                elem.append(x1)
                elem.append(x2)
                ust_table.append(elem)
            }

            while(document.querySelector("#ust_chart") != null){
                document.querySelector('#ust_chart').remove()
              }
            let chart = document.createElement('canvas')
            createChartBox(chart, data['val_stats'], data['ruparam'], data['tekparam'], data['location'])
            chart.id = 'ust_chart'
            ust_ch.append(chart)

        })
    }
})

// Фключение/Отключение активных зон распределения данных
function toggleData(num){
    console.log(num, mycharts.length)
    if (num == 0){
        for (i = 0; i < mycharts.length; i++){
            mycharts[i].options.plugins.horizontalArbitraryLine.gr = !mycharts[i].options.plugins.horizontalArbitraryLine.gr 
            mycharts[i].update()
        }
    }
    if (num == 1){
        for (i = 0; i < mycharts.length; i++){
            mycharts[i].options.plugins.horizontalArbitraryLine.yel = !mycharts[i].options.plugins.horizontalArbitraryLine.yel 
            mycharts[i].update()
        }
    }
    if (num == 2){
        for (i = 0; i < mycharts.length; i++){
            mycharts[i].options.plugins.horizontalArbitraryLine.rd = !mycharts[i].options.plugins.horizontalArbitraryLine.rd 
            mycharts[i].update()
        }
    }

}
