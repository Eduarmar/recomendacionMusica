package com.eduardo.cassandra.views;

import com.vaadin.flow.component.upload.Upload;
import com.vaadin.flow.component.upload.receivers.MemoryBuffer;
import com.vaadin.flow.component.dialog.Dialog;
import com.vaadin.flow.component.notification.Notification;

import com.eduardo.cassandra.CassandraConnection;
import com.vaadin.flow.component.button.Button;
import com.vaadin.flow.component.html.Div;
import com.vaadin.flow.component.html.H2;
import com.vaadin.flow.component.html.Paragraph;
import com.vaadin.flow.component.orderedlayout.HorizontalLayout;
import com.vaadin.flow.component.orderedlayout.VerticalLayout;
import com.vaadin.flow.router.Route;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;

@Route("")
public class MainView extends VerticalLayout {

    private final CassandraConnection cassandraConnection;

    @Autowired
    public MainView(CassandraConnection cassandraConnection) {
        this.cassandraConnection = cassandraConnection;

        add(new H2("🎧 Recomendaciones Musicales"));
        
        // ✅ Contenedor de botones (vertical)
        VerticalLayout botonesLayout = new VerticalLayout();
        botonesLayout.setPadding(false);
        botonesLayout.setSpacing(true);
        botonesLayout.getStyle().set("min-width", "220px");

        // ✅ Div de resultados (al costado)
        Div resultadosDiv = new Div();
        resultadosDiv.getStyle().set("padding", "15px");
        resultadosDiv.getStyle().set("border", "1px solid #ccc");
        resultadosDiv.getStyle().set("border-radius", "10px");
        resultadosDiv.getStyle().set("background-color", "#f9f9f9");
        resultadosDiv.getStyle().set("min-width", "500px");
        resultadosDiv.getStyle().set("max-width", "600px");

        // ✅ Layout horizontal: botones a la izquierda, resultados a la derecha
        HorizontalLayout layoutPrincipal = new HorizontalLayout(botonesLayout, resultadosDiv);
        layoutPrincipal.setAlignItems(Alignment.START);
        add(layoutPrincipal);

        // Boton Obtener todas las canciones
        Button cancionesButton = new Button("Mostrar canciones");
        add(cancionesButton);

        cancionesButton.addClickListener(e -> {
            resultadosDiv.removeAll();
            List<String> canciones = this.cassandraConnection.getTitulosCanciones();
            if (canciones.isEmpty()) {
                add(new Paragraph("⚠️ No hay canciones en la base de datos."));
            } else {
                canciones.forEach(c -> resultadosDiv.add(new Paragraph(c)));
            }
        });
        botonesLayout.add(cancionesButton);

        Button btnMostrarUsuarios = new Button("👥 Mostrar usuarios");
        Button btnMostrarEscuchas = new Button("🎧 Mostrar escuchas");

        btnMostrarUsuarios.addClickListener(e -> {
            resultadosDiv.removeAll();
            cassandraConnection.getUsuarios().forEach(usuario -> resultadosDiv.add(new Paragraph(usuario)));
        });

        btnMostrarEscuchas.addClickListener(e -> {
            resultadosDiv.removeAll();
            cassandraConnection.getEscuchas().forEach(escucha -> resultadosDiv.add(new Paragraph(escucha)));
        });

        botonesLayout.add(btnMostrarUsuarios, btnMostrarEscuchas); 

        // Boton para optener Top 3 canciones por genero
        Button top3Button = new Button("Top 3 por género");
            add(top3Button);

            top3Button.addClickListener(e -> {
                resultadosDiv.removeAll(); // limpia antes

                Map<String, List<String>> topPorGenero = cassandraConnection.getTop3PorGenero();

                if (topPorGenero.isEmpty()) {
                    resultadosDiv.add(new Paragraph("⚠️ No hay datos para mostrar."));
                } else {
                    topPorGenero.forEach((genero, canciones) -> {
                        resultadosDiv.add(new Paragraph("🎼 Género: " + genero));
                        canciones.forEach(info -> resultadosDiv.add(new Paragraph("   • " + info)));
                    });
                }
            });
            botonesLayout.add(top3Button);

        //Boton OLAP estadisticas de genero y mes
        Button olapButton = new Button("Estadísticas por género y mes");
        add(olapButton);

        olapButton.addClickListener(e -> {
            resultadosDiv.removeAll();

            List<String> estadisticas = cassandraConnection.getEstadisticasGeneroPorMes();

            if (estadisticas.isEmpty()) {
                resultadosDiv.add(new Paragraph("⚠️ No hay datos para mostrar."));
            } else {
                resultadosDiv.add(new Paragraph("📊 Estadísticas por Género y Mes:"));
                estadisticas.forEach(linea -> resultadosDiv.add(new Paragraph(linea)));
            }
        });

         botonesLayout.add(olapButton);

        //Boton OLAP estadisticas Distribucion de usuarios por cidudad
        Button ciudadButton = new Button("Distribución de usuarios por ciudad");
        add(ciudadButton);

        ciudadButton.addClickListener(e -> {
            resultadosDiv.removeAll();

            List<String> resultados = cassandraConnection.getDistribucionUsuariosPorCiudad();

            if (resultados.isEmpty()) {
                resultadosDiv.add(new Paragraph("⚠️ No hay usuarios registrados."));
            } else {
                resultadosDiv.add(new Paragraph("📊 Distribución de usuarios por ciudad:"));
                resultados.forEach(linea -> resultadosDiv.add(new Paragraph(linea)));
            }
        });

        botonesLayout.add(ciudadButton);

        //Boton OLAP estadisticas Usuarios escuchando mas de un genero
        Button multiGeneroButton = new Button("Usuarios con gustos variados");
        add(multiGeneroButton);

        multiGeneroButton.addClickListener(e -> {
            resultadosDiv.removeAll();

            List<String> resultados = cassandraConnection.getUsuariosMultiGenero();

            resultadosDiv.add(new Paragraph("🎯 Usuarios que escucharon más de un género distinto:"));
            resultados.forEach(linea -> resultadosDiv.add(new Paragraph(linea)));
        });

         botonesLayout.add(multiGeneroButton);

        //botom limpiar
        Button limpiarButton = new Button("🧹 Limpiar Tablas");
            limpiarButton.addClickListener(e -> {
                cassandraConnection.limpiarTablas();
                Notification.show("Tablas limpiadas exitosamente.");
            });
            botonesLayout.add(limpiarButton);
        
        
        //boton agregar Usuarios
        Button subirUsuariosBtn = new Button("📁 Cargar usuarios.csv");
        subirUsuariosBtn.addClickListener(e -> {
            Dialog dialog = new Dialog();
            dialog.setHeaderTitle("Subir archivo de usuarios");

            MemoryBuffer buffer = new MemoryBuffer();
            Upload upload = new Upload(buffer);
            upload.setAcceptedFileTypes(".csv");
            upload.addSucceededListener(event -> {
                cassandraConnection.cargarUsuariosDesdeCSV(buffer.getInputStream());
                Notification.show("✅ Usuarios cargados.");
                dialog.close();
            });

            dialog.add(upload);
            dialog.open();
        });
        botonesLayout.add(subirUsuariosBtn);

        //Boton cargar Canciones
        Button subirCancionesBtn = new Button("📁 Cargar canciones.csv");
        subirCancionesBtn.addClickListener(e -> {
            Dialog dialog = new Dialog();
            dialog.setHeaderTitle("Subir archivo de canciones");

            MemoryBuffer buffer = new MemoryBuffer();
            Upload upload = new Upload(buffer);
            upload.setAcceptedFileTypes(".csv");
            upload.addSucceededListener(event -> {
                cassandraConnection.cargarCancionesDesdeCSV(buffer.getInputStream());
                Notification.show("✅ Canciones cargadas.");
                dialog.close();
            });

            dialog.add(upload);
            dialog.open();
        });
        botonesLayout.add(subirCancionesBtn);



        Button subirEscuchasBtn = new Button("📁 Cargar escuchas.csv");
        subirEscuchasBtn.addClickListener(e -> {
            Dialog dialog = new Dialog();
            dialog.setHeaderTitle("Subir archivo de escuchas");

            MemoryBuffer buffer = new MemoryBuffer();
            Upload upload = new Upload(buffer);
            upload.setAcceptedFileTypes(".csv");
            upload.addSucceededListener(event -> {
                cassandraConnection.cargarEscuchasDesdeCSV(buffer.getInputStream());
                Notification.show("✅ Escuchas cargadas.");
                dialog.close();
            });

            dialog.add(upload);
            dialog.open();
        });
        botonesLayout.add(subirEscuchasBtn);


 
    }
}

