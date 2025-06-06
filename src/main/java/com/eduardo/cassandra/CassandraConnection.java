package com.eduardo.cassandra;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.HashSet;
import java.io.FileReader;
import java.net.InetSocketAddress;
import com.opencsv.CSVReader;
import java.io.FileReader;
import java.util.UUID;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.springframework.stereotype.Service;




@Service
public class CassandraConnection {

    private final CqlSession session;  
    
    public CassandraConnection(){
                
            session = CqlSession.builder()
                    .addContactPoint(new java.net.InetSocketAddress("127.0.0.1", 9042))
                    .withLocalDatacenter("datacenter1") 
                    .withKeyspace("recomendaciones")
                    .build();

            System.out.println("Conexión a Cassandra exitosa!");

    }

        //cargar Usuarios cvs
        public void cargarUsuariosDesdeCSV(InputStream inputStream) {
            try (CSVReader reader = new CSVReader(new InputStreamReader(inputStream))) {
                String[] nextLine;
                reader.readNext(); // Saltar encabezado

                while ((nextLine = reader.readNext()) != null) {
                    try {
                        int id = Integer.parseInt(nextLine[0].trim());
                        String nombre = nextLine[1];
                        String ciudad = nextLine[2];

                        // Escapar comillas simples en los valores
                        String safeNombre = nombre.replace("'", "''");
                        String safeCiudad = ciudad.replace("'", "''");

                        String query = String.format(
                            "INSERT INTO usuarios (usuario_id, nombre, ciudad) VALUES (%d, '%s', '%s');",
                            id, safeNombre, safeCiudad
                        );

                        session.execute(query);
                    } catch (Exception ex) {
                        System.err.println("❌ Error en línea: " + Arrays.toString(nextLine));
                        ex.printStackTrace();
                    }
                }

                System.out.println("✅ Usuarios cargados correctamente desde CSV.");
            } catch (Exception e) {
                System.err.println("❌ Error al leer el archivo CSV de usuarios:");
                e.printStackTrace();
            }
        }

        //cargar Canciones cvs
        public void cargarCancionesDesdeCSV(InputStream inputStream) {
            try (CSVReader reader = new CSVReader(new InputStreamReader(inputStream))) {
                String[] nextLine;
                reader.readNext(); // Saltar encabezado

                while ((nextLine = reader.readNext()) != null) {
                    try {
                        int id = Integer.parseInt(nextLine[0].trim());
                        String titulo = nextLine[1];
                        String artista = nextLine[2];
                        String genero = nextLine[3];

                        // Escapar comillas simples para evitar errores de sintaxis
                        String safeTitulo = titulo.replace("'", "''");
                        String safeArtista = artista.replace("'", "''");
                        String safeGenero = genero.replace("'", "''");

                        String query = String.format(
                            "INSERT INTO canciones (cancion_id, titulo, artista, genero) VALUES (%d, '%s', '%s', '%s');",
                            id, safeTitulo, safeArtista, safeGenero
                        );

                        session.execute(query);
                    } catch (Exception ex) {
                        System.err.println("❌ Error en línea: " + Arrays.toString(nextLine));
                        ex.printStackTrace();
                    }
                }

                System.out.println("✅ Canciones cargadas correctamente desde CSV.");
            } catch (Exception e) {
                System.err.println("❌ Error al leer el archivo CSV de canciones:");
                e.printStackTrace();
            }
        }

        //cargar Escuchas cvs
        public void cargarEscuchasDesdeCSV(InputStream inputStream) {
            try (CSVReader reader = new CSVReader(new InputStreamReader(inputStream))) {
                String[] nextLine;
                reader.readNext(); // skip header
                while ((nextLine = reader.readNext()) != null) {
                    int usuarioId = Integer.parseInt(nextLine[0].trim());
                    int cancionId = Integer.parseInt(nextLine[1].trim());
                    String fecha = nextLine[2];

                    session.execute(String.format(
                        "INSERT INTO escuchas (usuario_id, cancion_id, fecha_escucha) VALUES (%d, %d, '%s');",
                        usuarioId, cancionId, fecha
                    ));
                }
                System.out.println("✅ Escuchas cargadas desde CSV.");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        //limpiar tablas
        public void limpiarTablas() {
            try {
                session.execute("TRUNCATE recomendaciones.usuarios;");
                session.execute("TRUNCATE recomendaciones.canciones;");
                session.execute("TRUNCATE recomendaciones.escuchas;");
                System.out.println("🧹 Tablas vaciadas correctamente.");
            } catch (Exception e) {
                System.err.println("❌ Error al truncar tablas: " + e.getMessage());
            }
        }

        //Obtener Usuarios
        public List<String> getUsuarios() {
            List<String> resultados = new ArrayList<>();
            ResultSet rs = session.execute("SELECT nombre, ciudad FROM usuarios;");
            for (Row row : rs) {
                String nombre = row.getString("nombre");
                String ciudad = row.getString("ciudad");
                resultados.add("👤 " + nombre + " - " + ciudad);
            }
            return resultados;
        }

        //lista de escuchas
        public List<String> getEscuchas() {
            List<String> resultados = new ArrayList<>();
            ResultSet rs = session.execute("SELECT usuario_id, cancion_id, fecha_escucha FROM escuchas;");
            for (Row row : rs) {
                int usuarioId = row.getInt("usuario_id");
                int cancionId = row.getInt("cancion_id");
                Instant fecha = row.getInstant("fecha_escucha");
                resultados.add(String.format("🎧 %d | %d | %s", usuarioId, cancionId, fecha.toString()));
            }
            return resultados;
        }

        // Obtener lista completa de canciones
        public List<String> getTitulosCanciones() {

            List<String> resultados = new ArrayList<>();
        try {
            ResultSet resultSet = session.execute("SELECT titulo, artista, genero FROM canciones;");
            for (Row row : resultSet) {
                String titulo = row.getString("titulo");
                String artista = row.getString("artista");
                String genero = row.getString("genero");

                String descripcion = String.format(" %s | %s | %s", titulo, artista, genero);
                resultados.add(descripcion);
            }
        } catch (Exception e) {
            System.err.println("❌ Error al consultar canciones: " + e.getMessage());
        }
        return resultados;
    }


    //Top 3 canciones escuchadas por genero
     public Map<String, List<String>> getTop3PorGenero() {
        Map<Integer, Integer> contador = new HashMap<>();

        // 1. Obtener escuchas agrupadas por canción
        ResultSet escuchasRS = session.execute("SELECT cancion_id FROM escuchas;");
        for (Row row : escuchasRS) {
            int cancionId = row.getInt("cancion_id");
            contador.put(cancionId, contador.getOrDefault(cancionId, 0) + 1);
        }

        // 2. Agrupar por género y guardar info de canciones + reproducciones
        Map<String, List<String>> recomendacionesPorGenero = new HashMap<>();

        for (Map.Entry<Integer, Integer> entry : contador.entrySet()) {
            int cancionId = entry.getKey();
            int reproducciones = entry.getValue();

            String query = String.format("SELECT titulo, artista, genero FROM canciones WHERE cancion_id = %s;", cancionId);
            ResultSet cancionesRS = session.execute(query);

            for (Row songRow : cancionesRS) {
                String titulo = songRow.getString("titulo");
                String artista = songRow.getString("artista");
                String genero = songRow.getString("genero");

                String descripcion = String.format("🎵 %s - %s | Reproducciones: %d", titulo, artista, reproducciones);

                recomendacionesPorGenero
                    .computeIfAbsent(genero, k -> new ArrayList<>())
                    .add(descripcion);
            }
        }

        // 3. Ordenar cada lista de canciones por número de reproducciones (descendente) y limitar a 3
        for (Map.Entry<String, List<String>> entry : recomendacionesPorGenero.entrySet()) {
            List<String> canciones = entry.getValue();

            canciones.sort((a, b) -> {
                int repA = Integer.parseInt(a.replaceAll(".*Reproducciones: ", ""));
                int repB = Integer.parseInt(b.replaceAll(".*Reproducciones: ", ""));
                return Integer.compare(repB, repA);
            });

            // Limitar a top 3
            if (canciones.size() > 3) {
                recomendacionesPorGenero.put(entry.getKey(), canciones.subList(0, 3));
            }
        }

        return recomendacionesPorGenero;
    }

    // funcion para obtener OLAP de Genero por mes
    public List<String> getEstadisticasGeneroPorMes() {
        Map<String, Integer> escuchasPorGeneroMes = new HashMap<>();

        ResultSet escuchasRS = session.execute("SELECT cancion_id, fecha_escucha FROM escuchas;");
        for (Row row : escuchasRS) {
            int cancionId = row.getInt("cancion_id");

            // Obtener género
            String genero = "";
            String cancionQuery = String.format("SELECT genero FROM canciones WHERE cancion_id = %s;", cancionId);
            ResultSet generoRS = session.execute(cancionQuery);
            Row generoRow = generoRS.one();
            if (generoRow != null) {
                genero = generoRow.getString("genero");
            } else {
                continue;
            }

            // Obtener mes
            Instant fechaInst = row.getInstant("fecha_escucha");
            LocalDate fechaLocal = fechaInst.atZone(ZoneId.systemDefault()).toLocalDate();
            String mes = fechaLocal.getYear() + "-" + String.format("%02d", fechaLocal.getMonthValue());

            // Contar
            String clave = genero + "|" + mes;
            escuchasPorGeneroMes.put(clave, escuchasPorGeneroMes.getOrDefault(clave, 0) + 1);
        }

        // Convertir resultados a lista legible
        List<String> resultados = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : escuchasPorGeneroMes.entrySet()) {
            String[] partes = entry.getKey().split("\\|");
            String genero = partes[0];
            String mes = partes[1];
            int conteo = entry.getValue();
            resultados.add(String.format("📅 %s | 🎶 %s | 🔁 %d escuchas", mes, genero, conteo));
        }

        return resultados;
    }

    // OLAP Usuarios por ciudad
    public List<String> getDistribucionUsuariosPorCiudad() {
        List<String> resultados = new ArrayList<>();
        Map<String, Integer> usuariosPorCiudad = new HashMap<>();

        ResultSet usuariosRS = session.execute("SELECT ciudad FROM usuarios;");

        for (Row row : usuariosRS) {
            String ciudad = row.getString("ciudad");
            usuariosPorCiudad.put(ciudad, usuariosPorCiudad.getOrDefault(ciudad, 0) + 1);
        }

        for (Map.Entry<String, Integer> entry : usuariosPorCiudad.entrySet()) {
            String linea = String.format("📍 %s: %d usuarios", entry.getKey(), entry.getValue());
            resultados.add(linea);
        }

        return resultados;
    }

    // OLAP obtener Usuarios que escucharon mas de un genero
    public List<String> getUsuariosMultiGenero() {
        List<String> resultados = new ArrayList<>();

        // 1. Obtener nombres de usuarios
        Map<Integer, String> nombresUsuarios = new HashMap<>();
        ResultSet usuariosRS = session.execute("SELECT usuario_id, nombre FROM usuarios;");
        for (Row row : usuariosRS) {
            nombresUsuarios.put(row.getInt("usuario_id"), row.getString("nombre"));
        }

        // 2. Obtener géneros escuchados por usuario
        Map<Integer, HashSet<String>> generosPorUsuario = new HashMap<>();
        ResultSet escuchasRS = session.execute("SELECT usuario_id, cancion_id FROM escuchas;");
        for (Row row : escuchasRS) {
             int usuarioId = row.getInt("usuario_id");
             int cancionId = row.getInt("cancion_id");

            // Consultar el género de la canción
            String query = String.format("SELECT genero FROM canciones WHERE cancion_id = %s;", cancionId);
            ResultSet cancionRS = session.execute(query);
            Row generoRow = cancionRS.one();
            if (generoRow == null) continue;

            String genero = generoRow.getString("genero");
            generosPorUsuario.putIfAbsent(usuarioId, new HashSet<>());
            generosPorUsuario.get(usuarioId).add(genero);
        }

        // 3. Filtrar usuarios con más de un género
        int contador = 0;
        for (Map.Entry<Integer, HashSet<String>> entry : generosPorUsuario.entrySet()) {
            if (entry.getValue().size() > 1) {
                contador++;
                String nombre = nombresUsuarios.getOrDefault(entry.getKey(), "Desconocido");
                String line = String.format("👤 %s escuchó %d géneros distintos: %s",
                        nombre,
                        entry.getValue().size(),
                        String.join(", ", entry.getValue()));
                resultados.add(line);
            }
        }

        if (contador == 0) {
            resultados.add("😐 Ningún usuario escuchó más de un género distinto.");
        }

        return resultados;
    }
    
}



