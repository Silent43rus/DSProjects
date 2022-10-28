// Боковая панель

const apps = document.querySelector('#apps')
const topmenu = document.querySelector('.topmenu')
function openMenu(){
    document.querySelector('#sidebar').classList.toggle('active')
    document.querySelector('div[onclick="openMenu()"]').classList.toggle('active')
    document.querySelector('#main_form').classList.toggle('active')
    document.querySelector('#output_form').classList.toggle('active')
}

function getsubmenu(){

  document.querySelector('.submenu').classList.toggle('active')
}

apps.onclick = function (e) {
    const parent = e.target.parentNode
    if (e.target.tagName == 'A' && parent.classList.contains('header')){

        console.log(e.target.id)

        const items = document.querySelector(`div[id="${e.target.id}"]`).querySelectorAll('.body')
        for (elem of items){
            elem.classList.toggle('active')
        }
    }
    if (e.target.id.includes('app')){
        e.preventDefault()
    }
}

topmenu.onclick = function (e) {
    e.preventDefault()
}