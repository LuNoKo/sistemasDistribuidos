# Node.py
import socket
import threading
import pickle
import os
import sys
from collections import defaultdict

# --- CONFIGURAÇÕES GLOBAIS PARAMETRIZÁVEIS ---
FRAG_SIZE = 64  # Tamanho do fragmento em bytes
BUFFER_SIZE = 4096
IP = '127.0.0.1'

# Mapeamento do ID do Nó para a PORTA TCP
NODE_PORTS = {
    'A': 5001,
    'B': 5002,
    'C': 5003,
    'D': 5004,
}

# Mapeamento estático para simular a topologia em anel (ID -> ID do Sucessor)
# Isso permite que a topologia seja alterada centralmente.
RING_TOPOLOGY = {
    'A': 'B',  # A -> B
    'B': 'C',  # B -> C
    'C': 'D',  # C -> D
    'D': 'A'   # D -> A
}
# -----------------------------------------------

class DistributedNode:
    def __init__(self, node_id):
        self.node_id = node_id
        if node_id not in NODE_PORTS:
            raise ValueError(f"ID de nó '{node_id}' inválido. IDs válidos: {list(NODE_PORTS.keys())}")
        
        self.port = NODE_PORTS[node_id]
        sucessor_id = RING_TOPOLOGY[node_id]
        self.sucessor_port = NODE_PORTS[sucessor_id]
        self.addr = (IP, self.port)
        
        # { nome_arquivo: { pos_frag: { 'data': bytes, 'total': int } } }

        self.storage = defaultdict(dict)
        self.known_files = set()
        print(f"Nó {self.node_id} ({self.port}) iniciado. Sucessor: {sucessor_id} ({self.sucessor_port})")

    # [RESTO DA CLASSE DistributedNode PERMANECE O MESMO]
    # (As funções _connect_to_node, _get_active_successor, _send_to_successor,
    # handle_upload, handle_download, _request_fragments_from_ring,
    # handle_ring_request, handle_list, handle_client, e start_server
    # permanecem inalteradas, utilizando as variáveis de instância self.port
    # e self.sucessor_port, que agora são lidas das variáveis globais.)

    # Apenas para garantir a integridade, o restante das funções:
    
    def _connect_to_node(self, port_destino):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.connect((IP, port_destino))
            return s
        except ConnectionRefusedError:
            print(f"🚨 Falha ao conectar ao nó {port_destino}. Assumindo que caiu.")
            return None

    def _get_active_successor(self):
        current_id = RING_TOPOLOGY[self.node_id]
        attempts = 0
        while attempts < len(NODE_PORTS):
            current_port = NODE_PORTS[current_id]
            s = self._connect_to_node(current_port)
            if s:
                s.close()
                return current_port
            
            # Se falhou, move para o próximo nó no anel
            current_id = RING_TOPOLOGY.get(current_id, list(RING_TOPOLOGY.keys())[0])
            attempts += 1
        return None

    def _send_to_successor(self, data):
        port_destino = self._get_active_successor()
        if not port_destino:
            print("❌ Não há sucessores ativos para replicação.")
            return False

        s = self._connect_to_node(port_destino)
        if s:
            try:
                s.sendall(pickle.dumps(data))
                s.close()
                return True
            except Exception as e:
                print(f"Erro ao enviar para {port_destino}: {e}")
            finally:
                s.close()
        return False
    
    def _save_fragment(self, frag_data):
        name = frag_data['nome_arquivo']
        pos = frag_data['posicao']
        self.storage[name][pos] = {
            'data': frag_data['fragmento_bytes'],
            'total': frag_data['total_fragmentos']
        }
        self.known_files.add(name)
        print(f"💾 Fragmento {name} Posição {pos} armazenado localmente.")

    def handle_upload(self, data):
        file_name = data['nome_arquivo']
        file_content = data['conteudo_bytes']
        total_fragments = (len(file_content) + FRAG_SIZE - 1) // FRAG_SIZE
        
        for i in range(total_fragments):
            inicio = i * FRAG_SIZE
            fim = inicio + FRAG_SIZE
            frag_bytes = file_content[inicio:fim]
            
            fragment = {
                "comando": "REPLICAR_FRAGMENTO",
                "nome_arquivo": file_name,
                "posicao": i,
                "total_fragmentos": total_fragments,
                "fragmento_bytes": frag_bytes
            }
            
            self._save_fragment(fragment)
            self._send_to_successor(fragment)

        return {"status": "OK", "message": f"Arquivo '{file_name}' distribuído em {total_fragments} fragmentos e replicado."}

    def handle_download(self, file_name):
        requested_fragments = self._request_fragments_from_ring(file_name)
        
        if not requested_fragments:
            return {"status": "ERRO", "message": f"Arquivo '{file_name}' não encontrado ou fragmentos insuficientes."}

        total_fragments = requested_fragments[0]['total']
        
        if len(requested_fragments) != total_fragments:
            return {"status": "ERRO", "message": f"Arquivo '{file_name}' incompleto. Encontrado {len(requested_fragments)} de {total_fragments} fragmentos."}
        
        sorted_fragments = sorted(requested_fragments, key=lambda f: f['posicao'])
        remontado_bytes = b''.join(f['data'] for f in sorted_fragments)
        
        print(f"✅ Arquivo '{file_name}' remontado com sucesso.")
        return {"status": "OK", "file_name": file_name, "conteudo_bytes": remontado_bytes}

    def _request_fragments_from_ring(self, file_name):
        collected_fragments = []
        
        # 1. Busca local
        for pos, frag_data in self.storage.get(file_name, {}).items():
            collected_fragments.append({'posicao': pos, 'data': frag_data['data'], 'total': frag_data['total']})
            
        # 2. Inicia o pedido no anel
        ring_request = {
            "comando": "BUSCA_FRAGMENTO_ANEL",
            "nome_arquivo": file_name,
            "origem": self.port,
            "fragments_so_far": collected_fragments
        }
        
        try:
            port_destino = self._get_active_successor()
            s = self._connect_to_node(port_destino)
            if not s: return collected_fragments
            
            s.sendall(pickle.dumps(ring_request))
            s.settimeout(5.0) 
            full_response = b''
            while True:
                chunk = s.recv(BUFFER_SIZE)
                if not chunk: break
                full_response += chunk
            s.close()
            
            final_data = pickle.loads(full_response)
            return final_data.get('fragments', [])
            
        except Exception as e:
            print(f"Erro na busca no anel: {e}")
            return collected_fragments

    def handle_ring_request(self, data):
        if data['comando'] == "REPLICAR_FRAGMENTO":
            self._save_fragment(data)
            return
        
        if data['comando'] == "BUSCA_FRAGMENTO_ANEL":
            file_name = data['nome_arquivo']
            collected_fragments = data['fragments_so_far']
            origem_port = data['origem']

            for pos, frag_data in self.storage.get(file_name, {}).items():
                is_new = not any(f['posicao'] == pos for f in collected_fragments)
                if is_new:
                    collected_fragments.append({'posicao': pos, 'data': frag_data['data'], 'total': frag_data['total']})
            
            if self.sucessor_port == origem_port:
                # O ciclo terminou! Retorna a lista consolidada para a origem.
                print(f"🔁 Ciclo de busca finalizado. Retornando para {origem_port}.")
                # O nó que chamou esta função precisa devolver a resposta para o nó anterior
                return {"fragments": collected_fragments} 

            ring_request = {
                "comando": "BUSCA_FRAGMENTO_ANEL",
                "nome_arquivo": file_name,
                "origem": origem_port,
                "fragments_so_far": collected_fragments
            }
            
            port_destino = self._get_active_successor()
            if not port_destino: return 

            s = self._connect_to_node(port_destino)
            if s:
                s.sendall(pickle.dumps(ring_request))
                s.close()

    def handle_list(self):
        ring_request = {
            "comando": "LISTAR_ANEL",
            "origem": self.port,
            "files_so_far": list(self.known_files)
        }
        
        try:
            port_destino = self._get_active_successor()
            s = self._connect_to_node(port_destino)
            if not s: return {"status": "OK", "files": list(self.known_files)}
            
            s.sendall(pickle.dumps(ring_request))
            s.settimeout(5.0) 
            full_response = b''
            while True:
                chunk = s.recv(BUFFER_SIZE)
                if not chunk: break
                full_response += chunk
            s.close()
            
            final_data = pickle.loads(full_response)
            return {"status": "OK", "files": final_data.get('files', [])}
            
        except Exception as e:
            print(f"Erro na listagem no anel: {e}")
            return {"status": "OK", "files": list(self.known_files)}
            
    def handle_client(self, conn, addr):
        try:
            full_data = b''
            while True:
                chunk = conn.recv(BUFFER_SIZE)
                if not chunk: break
                full_data += chunk
            
            if not full_data: return
            
            data = pickle.loads(full_data)
            comando = data.get('comando')
            
            response = None
            
            if comando == "UPLOAD":
                print(f"UPLOAD")
                response = self.handle_upload(data)
            elif comando == "DOWNLOAD":
                response = self.handle_download(data['nome_arquivo'])
            elif comando == "LIST":
                response = self.handle_list()
            elif comando == "BUSCA_FRAGMENTO_ANEL":
                # Resposta para busca no anel
                response = self.handle_ring_request(data) 
            elif comando == "REPLICAR_FRAGMENTO":
                self.handle_ring_request(data)
                
            if response:
                conn.sendall(pickle.dumps(response))
                
        except Exception as e:
            print(f"Erro no manuseio da conexão de {addr}: {e}")
        finally:
            conn.close()

    def start_server(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(self.addr)
        server.listen(5)
        
        server.settimeout(10.0) 
        
        print(f"Servidor {self.node_id} ouvindo em {self.addr}")

        while True:
            try:
                conn, addr = server.accept()
                thread = threading.Thread(target=self.handle_client, args=(conn, addr))
                thread.start()
            
            except socket.timeout:
                pass

            
            except KeyboardInterrupt:
                print(f"\n🛑 Servidor {self.node_id} encerrado por KeyboardInterrupt.")
                break
            except Exception as e:
                print(f"Erro inesperado no loop principal: {e}")
                
        server.close()

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print(f"Uso: python Node.py <ID_DO_NÓ>")
        print(f"IDs disponíveis: {list(NODE_PORTS.keys())}")
        sys.exit(1)
        
    node_id = sys.argv[1].upper()
    try:
        node = DistributedNode(node_id)
        node.start_server()
    except ValueError as e:
        print(f"Erro de inicialização: {e}")
        sys.exit(1)