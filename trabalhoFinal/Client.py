# Client.py
import socket
import pickle
import os
import sys

# --- CONFIGURAÇÕES DA REQUISIÇÃO ---
NODE_TO_SEND_REQ = 'A' # A B C D
COMMAND = 'upload' # upload - download - list
FILE = 'teste.txt'


# --- CONFIGURAÇÕES GLOBAIS ---
NODE_PORTS = {
    'A': 5001,
    'B': 5002,
    'C': 5003,
    'D': 5004
}
IP = '127.0.0.1'
BUFFER_SIZE = 4096



def send_request(node_id, data):
    if node_id.upper() not in NODE_PORTS:
        return {"status": "ERRO", "message": f"ID de nó '{node_id}' desconhecido."}
        
    port = NODE_PORTS[node_id.upper()]
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect((IP, port))
        s.sendall(pickle.dumps(data))
        
        full_response = b''
        s.settimeout(10.0) 
        while True:
            chunk = s.recv(BUFFER_SIZE)
            if not chunk: break
            full_response += chunk
            
        if full_response:
            return pickle.loads(full_response)
        return {"status": "ERRO", "message": "Resposta vazia do nó."}
        
    except ConnectionRefusedError:
        return {"status": "ERRO", "message": f"Não foi possível conectar ao nó {node_id} ({port})."}
    except socket.timeout:
        return {"status": "ERRO", "message": "Tempo limite de resposta excedido."}
    except Exception as e:
        return {"status": "ERRO", "message": f"Erro inesperado: {e}"}
    finally:
        s.close()

def main():
    node_id = NODE_TO_SEND_REQ
    command = COMMAND
    dir = os.path.dirname(os.path.abspath(__file__)) + '\\'
    file_path = dir + FILE

    if command == "upload" and file_path:
        if not os.path.exists(file_path):
            print(f"Arquivo não encontrado: {file_path}")
            return

        file_name = os.path.basename(file_path)
        with open(file_path, 'rb') as f:
            content = f.read()

        request = {
            "comando": "UPLOAD",
            "nome_arquivo": file_name,
            "conteudo_bytes": content
        }

        print(f"📦 Enviando arquivo '{file_name}' ({len(content)} bytes) para o Nó {node_id}...")
        response = send_request(node_id, request)
        print(f"Resposta do Servidor: {response.get('message', 'Sem mensagem.')}")
    elif command == "download" and file_path:
        request = {
        "comando": "DOWNLOAD",
        "nome_arquivo": file_name
        }
        
        print(f"📥 Solicitando arquivo '{file_name}' ao Nó {node_id}...")
        response = send_request(node_id, request)
        
        if response.get("status") == "OK":
            content = response['conteudo_bytes']
            with open(f"RECUPERADO_{response['file_name']}", 'wb') as f:
                f.write(content)
            print(f"✅ Arquivo '{file_name}' recuperado e salvo como 'RECUPERADO_{response['file_name']}' ({len(content)} bytes).")
        else:
            print(f"❌ Erro ao baixar: {response.get('message', 'Erro desconhecido')}")
    elif command == "list":
        request = {"comando": "LIST"}

        print(f"📜 Solicitando lista de arquivos ao Nó {node_id}...")
        response = send_request(node_id, request)
        
        if response.get("status") == "OK":
            files = response.get('files', [])
            if files:
                print("\n--- Arquivos Disponíveis no Sistema ---")
                for f in sorted(files):
                    print(f"- {f}")
                print("--------------------------------------\n")
            else:
                print("Não há arquivos disponíveis no sistema.")
        else:
            print(f"❌ Erro ao listar: {response.get('message', 'Erro desconhecido')}")
    else:
        print(f"Comando '{command}' desconhecido ou faltando argumentos.")

if __name__ == '__main__':
    main()