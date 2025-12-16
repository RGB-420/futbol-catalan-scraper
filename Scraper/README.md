# 🧩 Base de Datos de Fútbol Amateur – PostgreSQL

Este proyecto define un modelo relacional en **PostgreSQL** para gestionar información de **competiciones de fútbol base/amateur**, incluyendo clubes, equipos, jugadores, cuerpo técnico, partidos, eventos y alineaciones.

---

## 📘 Estructura general

El modelo se organiza jerárquicamente:

```
Competiciones → Grupos → Equipos → Clubes
```

y se conecta con las entidades de **partidos**, **jugadores**, **cuerpo técnico**, **eventos** y **alineaciones**.

---

## 📚 Tablas principales

### 🏆 Competiciones
Contiene la información general de cada competición (por ejemplo, *Primera Catalana Cadete*).

| Campo | Tipo | Descripción |
|--------|------|--------------|
| `idCompeticion` | integer (PK) | Identificador único de la competición |
| `NombreCompeticion` | text | Nombre de la competición |
| `Categoria` | text | Categoría base (Juvenil, Cadete, etc.) |
| `EdadMaxima` | integer | Edad máxima permitida |
| `Organizador` | text | Federación u organismo responsable |
| `slug` | text | Identificador web o nombre corto |

---

### 🧭 Grupos
Cada competición puede dividirse en varios grupos.

| Campo | Tipo | Descripción |
|--------|------|--------------|
| `idGrupo` | integer (PK) | Identificador único del grupo |
| `idCompeticion` | integer (FK) | Relación con `Competiciones` |
| `NumeroGrupo` | integer | Número o código del grupo |
| `Temporada` | text | Temporada (ej. “2024-25”) |
| `Region` | text | Zona geográfica del grupo |
| `slug` | text | Código web o abreviatura |

---

### 🏟 Campos
Información sobre los estadios o campos donde se disputan los partidos.

| Campo | Tipo | Descripción |
|--------|------|--------------|
| `idCampo` | integer (PK) | Identificador único del campo |
| `NombreCampo` | text | Nombre del campo |
| `CodigoWeb` | integer | Código o ID web del campo |
| `Terreno` | text | Tipo de superficie (hierba, sintético, etc.) |
| `Direccion` | text | Dirección completa |
| `Localidad` | text | Ciudad o municipio |
| `Provincia` | text | Provincia o región |

---

### ⚽ Equipos
Define cada equipo (Cadete A, Juvenil B, etc.) perteneciente a un club y grupo.

| Campo | Tipo | Descripción |
|--------|------|--------------|
| `idEquipo` | integer (PK) | Identificador único del equipo |
| `idClub` | integer (FK) | Relación con `Clubes` |
| `idGrupo` | integer (FK) | Relación con `Grupos` |
| `Categoria` | text | Categoría del equipo |
| `Nivel` | integer | Nivel o división (1, 2, 3…) |

---

### 🏠 Clubes
Almacena la información general de cada club.

| Campo | Tipo | Descripción |
|--------|------|--------------|
| `idClub` | integer (PK) | Identificador único del club |
| `NombreClub` | text | Nombre completo del club |
| `Localidad` | text | Ciudad del club |
| `Delegacion` | text | Delegación o federación |
| `Provincia` | text | Provincia o región |
| `slug` | text | Identificador web o abreviatura |

---

### 🧑‍🤝‍🧑 Jugadores
Información básica de los jugadores (independiente del equipo actual).

| Campo | Tipo | Descripción |
|--------|------|--------------|
| `idJugador` | integer (PK) | Identificador único del jugador |
| `NombreJugador` | text | Nombre |
| `ApellidosJugador` | text | Apellidos |

#### 🔄 JugadoresEquipo
Tabla intermedia para representar los jugadores que pertenecen a un equipo.

| Campo | Tipo | Descripción |
|--------|------|--------------|
| `idJugadorEquipo` | integer (PK) | Identificador único |
| `idJugador` | integer (FK) | Relación con `Jugadores` |
| `idEquipo` | integer (FK) | Relación con `Equipos` |

---

### 🧑‍🏫 Cuerpo Técnico
Información de los miembros del staff (entrenadores, analistas, etc.).

| Campo | Tipo | Descripción |
|--------|------|--------------|
| `idStaff` | integer (PK) | Identificador único del técnico |
| `NombreStaff` | text | Nombre |
| `ApellidoStaff` | text | Apellidos |

#### 🔄 StaffEquipos
Tabla intermedia para vincular miembros del staff con equipos.

| Campo | Tipo | Descripción |
|--------|------|--------------|
| `idStaffEquipo` | integer (PK) | Identificador único |
| `idStaff` | integer (FK) | Relación con `Cuerpo Técnico` |
| `idEquipo` | integer (FK) | Relación con `Equipos` |
| `Rol` | text | Función dentro del cuerpo técnico |

---

### 🏁 Partidos
Registro de todos los partidos oficiales de la base de datos.

| Campo | Tipo | Descripción |
|--------|------|--------------|
| `idPartido` | integer (PK) | Identificador único |
| `idEquipoLocal` | integer (FK) | Equipo local |
| `idEquipoVisitante` | integer (FK) | Equipo visitante |
| `idCampo` | integer (FK) | Campo donde se juega |
| `idArbitro` | integer (FK) | Árbitro asignado |
| `idGrupo` | integer (FK) | Grupo al que pertenece el partido |
| `FechaPartido` | date | Fecha del partido |
| `HoraPartido` | time | Hora local |
| `GolesLocal` | integer | Goles del equipo local |
| `GolesVisitante` | integer | Goles del equipo visitante |
| `EstadoPartido` | text | Estado (pendiente, finalizado, suspendido...) |
| `Jornada` | integer | Número de jornada |

---

### 🧍‍♂️ Árbitros
| Campo | Tipo | Descripción |
|--------|------|--------------|
| `idArbitro` | integer (PK) | Identificador del árbitro |
| `NombreArbitro` | text | Nombre |
| `ApellidosArbitro` | text | Apellidos |
| `Delegacion` | text | Delegación o federación |

---

### 📊 Eventos
Registra goles, tarjetas y otros eventos del partido.

| Campo | Tipo | Descripción |
|--------|------|--------------|
| `idEvento` | integer (PK) | Identificador único |
| `idPartido` | integer (FK) | Partido en el que ocurre |
| `idEquipo` | integer (FK) | Equipo asociado |
| `idJugador` | integer (FK) | Jugador asociado |
| `Minuto` | integer | Minuto del evento |
| `TipoEvento` | text | Tipo de evento (“Gol”, “Amarilla”, “Roja”, etc.) |

---

### 🧾 Alineaciones
Registra la participación de jugadores en cada partido (titulares y suplentes).

| Campo | Tipo | Descripción |
|--------|------|--------------|
| `idAlineacion` | integer (PK) | Identificador único |
| `idJugador` | integer (FK) | Jugador que participa |
| `idEquipo` | integer (FK) | Equipo del jugador |
| `idPartido` | integer (FK) | Partido correspondiente |
| `Titular` | boolean | TRUE si fue titular, FALSE si suplente |
| `Dorsal` | integer | Número de camiseta (opcional) |

---

## 🔗 Diagrama relacional

- **Competiciones 1 → N Grupos**
- **Grupos 1 → N Equipos**
- **Clubes 1 → N Equipos**
- **Equipos 1 → N Partidos (como local/visitante)**
- **Equipos 1 → N JugadoresEquipo / StaffEquipos**
- **Jugadores 1 → N JugadoresEquipo / Alineaciones / Eventos**
- **Partidos 1 → N Eventos / Alineaciones**

---

## 🧠 Futuras ampliaciones

- Tabla `Sustituciones` (si algún día se pueden scrapear los cambios).
- Añadir `Temporada` en tablas intermedias (`JugadoresEquipo`, `StaffEquipos`, `Alineaciones`).
- Integrar scraping automático semanal con Python y almacenamiento incremental.
