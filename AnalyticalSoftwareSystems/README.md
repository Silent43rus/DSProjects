<h1>Аналитические программные системы</h1>
<ul>
    <h3><li>WEB</li></h3>
    <ul>
        <li>Аналитика аномалий технологических параметров (АТП)</li>
        <li>Аналитика воздействий автоматизированных правильных линий (АПЛ)</li>
    </ul>
    <h3><li>DESKTOP APP</li></h3>
    <ul>
        <li>Аналитика аномалий технологических параметров</li>
    </ul>
    <h4>Каждая система представляет из себя аналитическую платформу для 
        визуализации данных по работе непрерывных участков производства</h4>
    <h4>Описание системы АТП:</h4>
    <li>
        <h5>
            Представляет результаты работы участокв: 3 клети, 4-5 клети, резки, на ст 500, 
            результаты работы включают в себя сбор данных с контроллеров по технологических 
            параметрам (ТП) тока, скорости, вибрации и т.п их предобработки (определении наличия 
            заготовки на участке и расчет средних и максимальных значений ТП, дальнейшей оценки
            полученных значений от заранее разработанной модели по данному профилю и с последующим 
            экспортом результатов в PostgreSQL).
        </h5>
        <h5>
            Визуализация данных позволяет оператору и/или инженерно-техническому персоналу оценить
            уровень отклонений значений ТП от нормы как в реальном времени так и уже за прошедшие периоды
            проката.
        </h5>
    </li>   
    <h4>Описание системы АПЛ:</h4>
    <li>
        <h5>
            Представляет результаты работы участокв: 1, 2 и 3 линий правки цеха лифтовых направляющих, 
            результаты работы включают в себя сбор статистических данных с контроллеров для определения
            воздействий на заготовку и их последующий расчет для оценки производительности и качества 
            проделанной работы.
        </h5>
    </li>   
</ul>