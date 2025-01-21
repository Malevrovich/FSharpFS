# FSFS - FSharp FileSystem

`Скрябин Иван P34092 335146`

# Возможности
- FSFS в качестве постоянного хранилища данных и метаданных использует один файл/блочное устройство
- Внутри имеется кэш метаданных, представляющий из себя дерево файлов и каталог. Кэш данных отсутствует(однако в каком-то виде предоставлен ядром)
- По умолчанию всем файлам и каталогам владельцем устанавливается root, однако он при желании может это изменить при помощи `chown`. Управление доступом на уровне файловой системы не реализовано, она лишь хранит текущую маску(которую можно изменить при помощи `chmod`), проверкой наличия прав занимается ядро.
- FSFS позволяет создавать файлы и каталоги, со стандартным ограничением: длина имени не больше 255 символов.
- FSFS позволяет изменять размер, читать и записывать данные в файлы
- FSFS допускает многопоточный доступ. При возникновении конфликтов система остается в валидном состоянии(либо один из запросов выполняется, второй отклоняется, либо оба отклоняются)
- Операции над файловой системой реализованы таким образом, что при возникновении ошибок, они откатывают изменения и система остается в валидном состоянии.
- При непредвиденном отключении FSFS остается всегда в валидном состоянии
