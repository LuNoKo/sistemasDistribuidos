import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class HttpServer {
    public static void main(String[] args) throws Exception {
        try (ServerSocket ss = new ServerSocket(8088)) {
            System.out.println("Escutando em http://localhost:8088 ...");
            while(true){
                try (Socket s = ss.accept()) {
                    InputStream in = s.getInputStream();
                    InputStreamReader inR = new InputStreamReader(in);
                    BufferedReader br = new BufferedReader(inR);
                    OutputStream out = s.getOutputStream();
                    String line;

                    while ((line = br.readLine()) != null && !line.isEmpty()) {
                        System.out.println(line);
                    }

                    LocalDateTime now = LocalDateTime.now();
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");
                    String dateTime = now.format(formatter);

                    String responseBody = "<html>" + 
                                            "<body bgcolor=#333344 style='color:white; margin: 0; height: 100vh; display: flex; justify-content: center;align-items: center;'>" + 
                                                "<div style='text-align: center'>" + 
                                                    "<h1>Data e Hora Atual</h1>" +
                                                    "<p>" + dateTime + "</p>"+
                                                "</div>" + 
                                            "</body>" +
                                        "</html>";

                    String responseReader =
                        "HTTP/1.1 200 OK\r\n" +
                        "Content-Type: text/html; charset=UTF-8\r\n" +
                        "Content-Length: " + responseBody.getBytes().length + "\r\n" +
                        "\r\n";

                    String response = responseReader + responseBody;

                    out.write(response.getBytes("UTF-8"));
                    out.flush();
                }
            }
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}