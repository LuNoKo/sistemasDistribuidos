public class HttpResponse {

    private String responseHeader;
    private String responseBody;
    private byte[] responseFileContent;

    public HttpResponse(String header, String message) {
        this.responseHeader = header;
        this.responseBody = message;
        this.responseFileContent = null;
    }

    public HttpResponse(String header, byte[] fileContent) {
        this.responseHeader = header;
        this.responseBody = null;
        this.responseFileContent = fileContent;
    }

    public String getResponseHeader() {
        return responseHeader != null ? responseHeader : "";
    }

    public String getResponseBody() {
        return responseBody != null ? responseBody : "";
    }

    public byte[] getResponseFileContent() {
        return responseFileContent;
    }

    public boolean hasFileContent() {
        return responseFileContent != null;
    }
}