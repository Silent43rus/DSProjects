<h1>Система учета и управления ETL процессами</h1>
<h5>Функциональные возможности:</h5>
<ul>
    <li>Добавление / Удаление проектов</li>
    <li>Добавление / Удаление процессов</li>
    <li>Включение / Выключение процесса</li>
    <li>Просмотр логов всех процессов выбранного проекта</li>
    <li>Просмотр всех логов выбранного процесса</li>
    <li>Обновление процесса</li>
</ul>

<h5>Шаблон создания процессов</h5>
<ul>
    <li>
        <p>sys.path.insert(1, r'C:\Users\iba\ashihaldin')          # Путь к etl конструктору</p>
        <p>from ETLOmzСonstruct import Construct</p>
    </li>
    <p>В конструкторе класса:</p>
    <li>
        <p>self.script_name = os.path.basename(__file__)[:-3]</p>
        <p></p>self.file_directory = abspath(inspect.getsourcefile(lambda:0)).split(os.sep)[-3]</p>
    </li>
        <p>Далее в методе main() определяем основной алгоритм работы программы</p>
        <p>В ключевых местах (где интерация закончилась с тем или иным результатом) вызываем:</p>
        <p>Для записи логов</p>
    <li>
        <p>self.logWritter(self.script_name, self.file_directory, self.log_stat[2])</p>
        <p>,где log_stat[2] - результат выполнения итерации</p>
    </li>
        <p>Для обнавление статуса процесса</p>
    <li>
        <p>self.statusUpdater(self.script_name, self.file_directory, 2)</p>
        <p>, где 2 - статус выполнения итерации</p>
    </li>
</uL>
<h5>Статусы выполнения:</h5>
<ul>
    <li>0 - Процесс выключен</li>
    <li>1 - Процесс работает исправно</li>
    <li>2 - Процесс работает с ошибками</li>
    <li>3 - Процесс не работает</li>
</ul>