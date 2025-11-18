package aula;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HttpServer {

    private static final String DOCUMENT_ROOT = "public";
    private static final int BUFFER_SIZE = 1024;
    private static final String DEFAULT_INDEX_FILE = "index.html";

    public static void main(String[] args) {
        File rootDir = new File(DOCUMENT_ROOT);
        if (!rootDir.exists()) {
            rootDir.mkdirs();
            System.out.println("Diretório raiz criado: /" + DOCUMENT_ROOT);
        }

        try (ServerSocket ss = new ServerSocket(8088)) {
            System.out.println("Escutando em http://localhost:8088 ...");
            while (true) {
                try (Socket s = ss.accept()) {
                    handleRequest(s);
                } catch (Exception e) {
                    System.err.println("Erro ao processar a requisição: " + e.getMessage());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void handleRequest(Socket s) throws Exception {
        InputStream in = s.getInputStream();
        InputStreamReader inR = new InputStreamReader(in);
        BufferedReader br = new BufferedReader(inR);
        OutputStream out = s.getOutputStream();

        String firstLine = br.readLine();
        if (firstLine == null)
            return;

        System.out.println("--- Nova Requisição ---");
        System.out.println(firstLine);

        // Pattern pattern = Pattern.compile("^(\\w+)\\s+([^\\s]+)\\s+HTTP/.*");
        // Matcher matcher = pattern.matcher(firstLine);

        // if (!matcher.find()) {
        // sendErrorResponse(out, 400, "Bad Request", "Requisição HTTP Inválida.");
        // return;
        // }

        // // Recupera somente o caminho da requisição
        // String uri = matcher.group(2);

        String[] parts = firstLine.split(" ");
        String uri = parts.length > 1 ? parts[1] : "/";

        // Remove a barra inicial da URI
        String requestedPath = uri.equals("/") ? "" : uri.substring(1);
        File requestedFile = new File(DOCUMENT_ROOT, requestedPath);

        if (requestedFile.isDirectory() || requestedPath.isEmpty()) {
            File indexFile;
            if (requestedPath.isEmpty()) {
                indexFile = new File(DOCUMENT_ROOT, DEFAULT_INDEX_FILE);
            } else {
                indexFile = new File(requestedFile, DEFAULT_INDEX_FILE);
            }

            if (indexFile.exists() && !indexFile.isDirectory()) {
                // Servindo index
                handleFileRequest(out, indexFile);
            } else {
                // Servindo listagem já que o index nàp existe
                handleDirectoryListing(out, requestedFile, uri);
            }
        } else if (requestedFile.exists() && !requestedFile.isDirectory()) {
            // Servindo arquivo pois é um arquivo específico
            handleFileRequest(out, requestedFile);
        } else {
            // 404 caso não for encontrado
            sendErrorResponse(out, 404, "Not Found", "O recurso " + uri + " não foi encontrado.");
        }
    }

    private static void handleFileRequest(OutputStream out, File file) throws Exception {
        String fileName = file.getName();
        String contentType = getContentType(fileName);

        long fileLength = file.length();
        String header = "HTTP/1.1 200 OK\r\n" +
                "Content-Type: " + contentType + "\r\n" +
                "Content-Length: " + fileLength + "\r\n" +
                "\r\n";
        out.write(header.getBytes("UTF-8"));

        try (FileInputStream fis = new FileInputStream(file)) {
            byte[] buffer = new byte[BUFFER_SIZE];
            int bytesRead;
            while ((bytesRead = fis.read(buffer)) != -1) {
                out.write(buffer, 0, bytesRead);
            }
        }
        out.flush();
    }

    private static void handleDirectoryListing(OutputStream out, File directory, String currentUri) throws Exception {
        File[] files = directory.listFiles();
        StringBuilder body = new StringBuilder();

        if (!currentUri.endsWith("/")) {
            currentUri = currentUri + "/";
        }

        body.append("<html><head><title>Índice de ")
                .append(directory.getName().isEmpty() ? "/" : directory.getName())
                .append("</title>");
        body.append(
                "<style>body{font-family: Arial, sans-serif;} a{color: #0066cc; text-decoration: none;} a:hover{text-decoration: underline;} li { margin: 5px 0; list-style-type: none; }</style>");
        body.append("</head><body>");
        body.append("<h1>Conteúdo de: ").append(directory.getName().isEmpty() ? "/" : directory.getName())
                .append("</h1>");
        body.append("<ul>");

        // Link para voltar
        if (!directory.getPath().equals(DOCUMENT_ROOT)) {
            String parentPath = new File(DOCUMENT_ROOT).equals(directory.getParentFile()) ? "/" : "../";
            body.append("<li style='padding-bottom: 20px;'>&#128072; <a href=\"")
                    .append(parentPath)
                    .append("\"><b>Voltar</b></a></li>");
        }

        if (files.length == 0) {
            body.append("<li><b>Diretório vazio</b></li>");
            sendSuccessResponse(out, "text/html", body.toString().getBytes("UTF-8"));
        }

        if (files != null) {
            String name;
            String path;

            for (File file : files) {
                name = file.getName();
                path = currentUri + name;
                if (file.isDirectory() && !file.getName().isEmpty()) {
                    body.append("<li>&#x1F4C1; <a href=\"")
                            .append(path)
                            .append("/\">")
                            .append(name)
                            .append("</a></li>");
                } else if (file.isFile() && !file.getName().isEmpty()) {
                    body.append("<li>&#128462; <a href=\"")
                            .append(path)
                            .append("\">")
                            .append(name)
                            .append("</a></li>");
                }
            }
        }

        body.append("</ul></body></html>");

        sendSuccessResponse(out, "text/html", body.toString().getBytes("UTF-8"));
    }

    private static void sendSuccessResponse(OutputStream out, String contentType, byte[] content) throws Exception {
        String header = "HTTP/1.1 200 OK\r\n" +
                "Content-Type: " + contentType + "; charset=UTF-8\r\n" +
                "Content-Length: " + content.length + "\r\n" +
                "\r\n";
        out.write(header.getBytes("UTF-8"));
        out.write(content);
        out.flush();
    }

    private static void sendErrorResponse(OutputStream out, int statusCode, String statusText, String message)
            throws Exception {
        System.err.println("Erro " + statusCode + ": " + message);

        String responseBody = "<html><body style='font-family: Arial; text-align: center; color: #ff4444;'>" +
                "<h1>Erro " + statusCode + " - " + statusText + "</h1>" +
                "<p>" + message + "</p>" +
                "</body></html>";

        String header = "HTTP/1.1 " + statusCode + " " + statusText + "\r\n" +
                "Content-Type: text/html; charset=UTF-8\r\n" +
                "Content-Length: " + responseBody.getBytes("UTF-8").length + "\r\n" +
                "Connection: close\r\n" +
                "\r\n";

        out.write(header.getBytes("UTF-8"));
        out.write(responseBody.getBytes("UTF-8"));
        out.flush();
    }

    private static String getContentType(String fileName) {
        if (fileName.endsWith(".html") || fileName.endsWith(".htm"))
            return "text/html";
        if (fileName.endsWith(".css"))
            return "text/css";
        if (fileName.endsWith(".js"))
            return "application/javascript";
        if (fileName.endsWith(".jpg") || fileName.endsWith(".jpeg"))
            return "image/jpeg";
        if (fileName.endsWith(".png"))
            return "image/png";
        if (fileName.endsWith(".gif"))
            return "image/gif";
        if (fileName.endsWith(".ico"))
            return "image/x-icon";
        if (fileName.endsWith(".pdf"))
            return "application/pdf";

        return "application/octet-stream";
    }
}