# Node.py
import socket
import select
import pickle
import sys
from collections import defaultdict

FRAG_SIZE = 64
BUFFER_SIZE = 4096
IP = '127.0.0.1'

NODE_PORTS = {
    'A': 5001, 
    'B': 5002, 
    'C': 5003, 
    'D': 5004
}

RING_TOPOLOGY = {
    'A': 'B', 
    'B': 'C', 
    'C': 'D', 
    'D': 'A'
}

# --- VARIÁVEIS GLOBAIS (Estado do Nó) ---
MY_ID = ""
MY_PORT = 0
STORAGE = defaultdict(dict) # {arquivo: {pos: {'data': b'', 'total': n}}}
KNOWN_FILES = set()

# --- FUNÇÕES AUXILIARES ---

def get_successor_port():
    """Retorna a porta do próximo nó ativo."""
    next_id = RING_TOPOLOGY[MY_ID]
    # Tenta conectar no sucessor imediato. Se falhar, pula para o próximo (lógica simplificada)
    for _ in range(len(NODE_PORTS)):
        port = NODE_PORTS[next_id]
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(0.5)
            s.connect((IP, port))
            s.close()
            return port
        except:
            # Se falhar, tenta o próximo do anel
            next_id = RING_TOPOLOGY.get(next_id, 'A')
    return None

def send_to_node(port, data):
    """Envia dados para outro nó."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((IP, port))
        s.sendall(pickle.dumps(data))
        s.close()
    except Exception as e:
        print(f"Erro ao enviar para {port}: {e}")

def save_fragment(data):
    """Salva o fragmento na memória local."""
    name = data['nome_arquivo']
    pos = data['posicao']
    STORAGE[name][pos] = {
        'data': data['fragmento_bytes'],
        'total': data['total_fragmentos']
    }
    KNOWN_FILES.add(name)
    print(f"💾 Fragmento {name} [{pos}] salvo.")

# --- LÓGICA DE NEGÓCIO ---

def processar_upload(data):
    fname = data['nome_arquivo']
    content = data['conteudo_bytes']
    total = (len(content) + FRAG_SIZE - 1) // FRAG_SIZE
    
    # Quebra e distribui
    for i in range(total):
        frag_data = {
            "comando": "REPLICAR",
            "nome_arquivo": fname,
            "posicao": i,
            "total_fragmentos": total,
            "fragmento_bytes": content[i*FRAG_SIZE : (i+1)*FRAG_SIZE]
        }
        save_fragment(frag_data) # Salva cópia local
        
        succ_port = get_successor_port()
        if succ_port: send_to_node(succ_port, frag_data) # Manda pro anel

    return {"status": "OK", "msg": f"Upload '{fname}' iniciado."}

def processar_download(fname):
    # 1. Pega o que tem localmente
    frags = []
    for pos, info in STORAGE.get(fname, {}).items():
        frags.append({'posicao': pos, 'data': info['data'], 'total': info['total']})
    
    # 2. Se não tiver tudo, pede pro anel
    if not frags or len(frags) < frags[0]['total']:
        req = {
            "comando": "BUSCAR_ANEL",
            "nome_arquivo": fname,
            "origem": MY_PORT,
            "fragments": frags
        }
        succ = get_successor_port()
        if succ:
            # Conexão síncrona para esperar a resposta do anel
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((IP, succ))
            s.sendall(pickle.dumps(req))
            resp_bytes = b""
            while True:
                chunk = s.recv(4096)
                if not chunk: break
                resp_bytes += chunk
            s.close()
            frags = pickle.loads(resp_bytes).get('fragments', [])

    # 3. Tenta montar
    if not frags: return {"status": "ERRO", "msg": "Arquivo não encontrado."}
    
    frags.sort(key=lambda x: x['posicao'])
    if len(frags) != frags[0]['total']:
        return {"status": "ERRO", "msg": "Arquivo incompleto."}
        
    full_content = b''.join(f['data'] for f in frags)
    print(f"✅ Download '{fname}' concluído.")
    return {"status": "OK", "conteudo_bytes": full_content}

def processar_msg_anel(data):
    cmd = data['comando']
    
    if cmd == "REPLICAR":
        save_fragment(data) # Apenas salva e para (simplificação: replicação total)
        
    elif cmd == "BUSCAR_ANEL":
        fname = data['nome_arquivo']
        collected = data['fragments']
        origin = data['origem']
        
        # Adiciona o que eu tenho
        for pos, info in STORAGE.get(fname, {}).items():
            if not any(f['posicao'] == pos for f in collected):
                collected.append({'posicao': pos, 'data': info['data'], 'total': info['total']})
        
        # Se voltou pra origem, retorna
        if get_successor_port() == origin or MY_PORT == origin: # Lógica simples de parada
            return {"fragments": collected}
            
        # Repassa pro próximo
        succ = get_successor_port()
        if succ:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((IP, succ))
            data['fragments'] = collected # Atualiza lista
            s.sendall(pickle.dumps(data))
            # Espera resposta recursiva do anel
            resp = b""
            while True:
                c = s.recv(4096)
                if not c: break
                resp += c
            s.close()
            return pickle.loads(resp) # Retorna pra quem chamou antes

# --- MAIN (Execução Principal) ---

if __name__ == '__main__':
    if len(sys.argv) < 2: exit("Use: python Node.py <ID>")
    
    MY_ID = sys.argv[1].upper()
    MY_PORT = NODE_PORTS[MY_ID]
    
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((IP, MY_PORT))
    server.listen(5)
    server.setblocking(False)
    
    inputs = [server]
    print(f"🚀 Nó {MY_ID} rodando na porta {MY_PORT}")

    while True:
        try:
            # O timeout de 1.0 permite o Ctrl+C funcionar
            readable, _, _ = select.select(inputs, [], [], 1.0)

            for s in readable:
                if s is server:
                    # Nova conexão
                    conn, addr = s.accept()
                    conn.settimeout(5.0) # Timeout vital para não travar se cliente cair
                    inputs.append(conn)
                else:
                    # Recebendo dados
                    try:
                        full_data = b""
                        while True:
                            chunk = s.recv(BUFFER_SIZE)
                            if not chunk: break
                            full_data += chunk
                            if len(chunk) < BUFFER_SIZE: break
                        
                        if full_data:
                            req = pickle.loads(full_data)
                            resp = None
                            
                            if req['comando'] == 'UPLOAD':
                                resp = processar_upload(req)
                            elif req['comando'] == 'DOWNLOAD':
                                resp = processar_download(req['nome_arquivo'])
                            elif req['comando'] in ['REPLICAR', 'BUSCAR_ANEL']:
                                resp = processar_msg_anel(req)
                                
                            if resp: s.sendall(pickle.dumps(resp))
                    except Exception as e:
                        print(f"Erro processando cliente: {e}")
                    finally:
                        inputs.remove(s)
                        s.close()

        except KeyboardInterrupt:
            print("\n🛑 Encerrando nó...")
            break
        except Exception as e:
            print(f"Erro fatal no loop: {e}")
            break
            
    server.close()