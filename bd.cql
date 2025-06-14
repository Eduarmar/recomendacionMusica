CREATE KEYSPACE recomendaciones
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE recomendaciones.usuarios (
    usuario_id INT PRIMARY KEY,
    nombre TEXT,
    ciudad TEXT
);

CREATE TABLE recomendaciones.canciones (
    cancion_id INT PRIMARY KEY,
    titulo TEXT,
    artista TEXT,
    genero TEXT
);

CREATE TABLE recomendaciones.escuchas (
    usuario_id INT,
    cancion_id INT,
    fecha_escucha TIMESTAMP,
    PRIMARY KEY (usuario_id, cancion_id, fecha_escucha)
);
