# Диаграммы архитектуры Field Generator

Диаграммы в нотации **C4 Model** и UML Deployment.

## Файлы

| Файл | Уровень C4 | Содержание |
|------|------------|------------|
| `architecture.puml` | Context (L1) | Система, пользователь, внешние системы |
| `architecture-container.puml` | Container (L2) | Web UI, Backend API, СУБД |
| `architecture-layers.puml` | Component (L3) | Слои Backend: Presentation, Business, Data Access |
| `architecture-deployment.puml` | Deployment | Развёртывание: хост, Docker, порты |

## Просмотр

1. **plantuml.com** — скопировать содержимое файла, вставить в редактор
2. **VS Code** — плагин "PlantUML", Alt+D для превью
3. **IntelliJ** — плагин PlantUML integration

Диаграммы C4 используют внешний include; требуется доступ в интернет при рендеринге.
