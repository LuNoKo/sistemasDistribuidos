import socket
import pickle
import os
import sys

# --- CONFIGURAÇÕES ---
COMMAND = 'download'     # upload, download, list_all
NODE_TO_SEND_REQ = 'B' # Mude para A, B, C ou D para testar
FILE = 'teste.txts'     # Nome do arquivo na mesma pasta do script

NODE_PORTS = {
    'A': 5001, 'B': 5002, 'C': 5003, 'D': 5004
}
IP = '127.0.0.1'
BUFFER_SIZE = 4096

def send_request(node_id, data):
    if node_id.upper() not in NODE_PORTS:
        return {"status": "ERRO", "message": "ID desconhecido."}
        
    port = NODE_PORTS[node_id.upper()]
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect((IP, port))
        s.sendall(pickle.dumps(data))
        
        s.settimeout(5.0)
        
        full_response = b''
        while True:
            try:
                chunk = s.recv(BUFFER_SIZE)
                if not chunk: break
                full_response += chunk
            except socket.timeout:
                break # Se estourar tempo, assume que acabou ou travou
                
        if full_response:
            return pickle.loads(full_response)
        return {"status": "ERRO", "message": "Sem resposta."}
        
    except Exception as e:
        return {"status": "ERRO", "message": f"Erro de conexão: {e}"}
    finally:
        s.close()

def main():
    comando = sys.argv[1] if len(sys.argv) > 2  else COMMAND
    no_para_mandar_req = sys.argv[2].upper() if len(sys.argv) > 3 else NODE_TO_SEND_REQ
    arquivo = sys.argv[3] if len(sys.argv) == 4 else FILE

    dir_atual = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(dir_atual, arquivo)
    
    # --- UPLOAD ---
    if comando == "upload":
        with open(file_path, 'rb') as f:
            content = f.read()

        request = {
            "comando": "UPLOAD",
            "nome_arquivo": arquivo,
            "conteudo_bytes": content
        }
        print(f"📦 UPLOAD '{arquivo}' -> Nó {no_para_mandar_req}...")
        resp = send_request(no_para_mandar_req, request)
        print(f"Resposta: {resp['msg']}")

    # --- DOWNLOAD ---
    elif comando == "download":
        request = {
            "comando": "DOWNLOAD",
            "nome_arquivo": arquivo
        }
        print(f"📥 DOWNLOAD '{arquivo}' <- Nó {no_para_mandar_req}...")
        resp = send_request(no_para_mandar_req, request)
        
        if resp.get("status") == "OK":
            nome_salvo = f"RECUPERADO_{resp['nome_arquivo']}"
            with open(os.path.join(dir_atual, nome_salvo), 'wb') as f:
                f.write(resp['conteudo_bytes'])
            print(f"✅ Sucesso! Salvo como '{nome_salvo}'")
        else:
            resp = send_request(no_para_mandar_req, request)
            print(f"Erro: {resp['msg']}")

    # --- LISTAR ---
    elif comando == "list_all":
        print(f"📜 LISTANDO arquivos no Nó A...")
        resp = send_request('A', {"comando": "LISTAR_FRAGMENTOS"})
        print(resp)

        print(f"📜 LISTANDO arquivos no Nó B...")
        resp = send_request('B', {"comando": "LISTAR_FRAGMENTOS"})
        print(resp)

        print(f"📜 LISTANDO arquivos no Nó C...")
        resp = send_request('C', {"comando": "LISTAR_FRAGMENTOS"})
        print(resp)

        print(f"📜 LISTANDO arquivos no Nó D...")
        resp = send_request('D', {"comando": "LISTAR_FRAGMENTOS"})
        print(resp)

if __name__ == '__main__':
    main()