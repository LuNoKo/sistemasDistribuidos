import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class HttpServer {

    private static final String STATIC_DIR = "public";

    public static void main(String[] args) throws Exception {
        try (ServerSocket ss = new ServerSocket(8088)) {
            System.out.println("Escutando em http://localhost:8088 ...");
            while (true) {
                try (Socket s = ss.accept()) {
                    InputStream in = s.getInputStream();
                    InputStreamReader inR = new InputStreamReader(in);
                    BufferedReader br = new BufferedReader(inR);
                    OutputStream out = s.getOutputStream();

                    // Imprime todas as linhas
                    // String line;
                    // while ((line = br.readLine()) != null && !line.isEmpty()) {
                    // System.out.println(line);
                    // }

                    String requestLine = br.readLine();
                    if (requestLine == null)
                        continue;

                    // Imprime somente a primeira linha
                    System.out.println(requestLine);

                    String[] parts = requestLine.split(" ");
                    String path = parts.length > 1 ? parts[1] : "/";

                    HttpResponse response;
                    if ("/".equals(path)) {
                        response = serveDateTimePage(out);
                    } else {
                        response = serveStaticFile(path, out);
                    }

                    out.write((response.getResponseHeader() + response.getResponseBody()).getBytes("UTF-8"));
                    if (response.hasFileContent()) {
                        out.write(response.getResponseFileContent());
                    }
                    out.flush();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static HttpResponse serveDateTimePage(OutputStream out) throws Exception {
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");
        String dateTime = now.format(formatter);

        String responseBody = "<html>" +
                "<body bgcolor=#333344 style='color:white; margin: 0; height: 100vh; display: flex; justify-content: center;align-items: center;'>"
                +
                "<div style='text-align: center'>" +
                "<h1>Data e Hora Atual</h1>" +
                "<p>" + dateTime + "</p>" +
                "</div>" +
                "</body>" +
                "</html>";

        String responseHeader = "HTTP/1.1 200 OK\r\n" +
                "Content-Type: text/html; charset=UTF-8\r\n" +
                "Content-Length: " + responseBody.getBytes("UTF-8").length + "\r\n" +
                "\r\n";

        return new HttpResponse(responseHeader, responseBody);
    }

    private static HttpResponse serveStaticFile(String path, OutputStream out) throws Exception {
        Path filePath = Paths.get(STATIC_DIR, path);
        File file = filePath.toFile();

        if (file.exists() && !file.isDirectory()) {
            String mimeType = Files.probeContentType(filePath);
            if (mimeType == null) {
                mimeType = "application/octet-stream";
            }

            try (FileInputStream fis = new FileInputStream(file)) {
                byte[] fileContent = new byte[(int) file.length()];
                fis.read(fileContent);

                String responseHeader = "HTTP/1.1 200 OK\r\n" +
                        "Content-Type: " + "text/html" + "\r\n" +
                        "Content-Length: " + file.length() + "\r\n" +
                        "\r\n";

                return new HttpResponse(responseHeader, fileContent);
            }
        } else {
            String notFoundBody = "<html><body><h1>404 Not Found</h1><p>The requested URL " + path
                    + " was not found on this server.</p></body></html>";
            String notFoundHeader = "HTTP/1.1 404 Not Found\r\n" +
                    "Content-Type: text/html; charset=UTF-8\r\n" +
                    "Content-Length: " + notFoundBody.getBytes("UTF-8").length + "\r\n" +
                    "\r\n";

            return new HttpResponse(notFoundHeader, notFoundBody);
        }
    }
}
