import socket
import pickle
import sys
import os

# --- CONFIGURAÇÕES BÁSICAS ---
# Dicionário simples: Quem é o próximo?
QUEM_E_O_PROXIMO = {
    'A': 'B',
    'B': 'C',
    'C': 'D',
    'D': 'A'
}

# Portas de cada um
PORTAS = {
    'A': 5001,
    'B': 5002,
    'C': 5003,
    'D': 5004
}

MEU_IP = '127.0.0.1'
BUFFER_SIZE = 4096
FRAG_SIZE = 64

# --- ARMAZENAMENTO NA MEMÓRIA ---
# Guarda os arquivos assim: {'trabalho.txt': b'conteudo do arquivo...'}
MEUS_ARQUIVOS = []

# --- INÍCIO DO PROGRAMA ---

# 1. Pega o ID (A, B, C ou D) que passamos no terminal
if len(sys.argv) < 2:
    print("Erro! Use assim: python Node.py A")
    sys.exit()

MEU_ID = sys.argv[1].upper()
MINHA_PORTA = PORTAS[MEU_ID]
# PROXIMO_ID = QUEM_E_O_PROXIMO[MEU_ID]
# PORTA_DO_PROXIMO = PORTAS[PROXIMO_ID]

print(f"--- NÓ {MEU_ID} INICIADO ---")
print(f"Escutando na porta: {MINHA_PORTA}")
# print(f"Meu vizinho é o nó: {PROXIMO_ID} (Porta {PORTA_DO_PROXIMO})")
print("---------------------------")

# 2. Cria o servidor para escutar conexões
servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
servidor.bind((MEU_IP, MINHA_PORTA))
servidor.listen(5) # Aceita 1 conexão por vez (simples)

def get_porta_proximo_node():
    """Retorna a porta do próximo nó ativo."""
    proximo_id = QUEM_E_O_PROXIMO[MEU_ID]
    # Tenta conectar no sucessor imediato. Se falhar, pula para o próximo (lógica simplificada)
    for _ in range(len(PORTAS)):
        porta_proximo_id = PORTAS[proximo_id]
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(0.5)
            s.connect((MEU_IP, porta_proximo_id))
            s.close()
            return porta_proximo_id
        except:
            porta_proximo_id = QUEM_E_O_PROXIMO.get(porta_proximo_id, 'A')
    print("Nenhum outro nó está online...")
    return None


def enviar_para_node(porta_proximo_node, data):
    """Envia dados para outro nó."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((MEU_IP, porta_proximo_node))
        s.sendall(pickle.dumps(data))
        
    except Exception as e:
        print(f"Erro ao enviar para {porta_proximo_node}: {e}")

def imprimir_arquivos_do_nodo():
    print("\n" + "="*14 + f" Fragmentos no nodo {MEU_ID} " + "="*14)
    print(f"{'ARQUIVO':<20} | {'POS':<5} | {'TOTAL':<5} | {'TAMANHO'}")
    print("-" * 50)
    
    if not MEUS_ARQUIVOS:
        print("   (Nenhum arquivo na memória)")
    
    for item in MEUS_ARQUIVOS:
        nome = item['nome_arquivo']
        pos = item['posicao']
        total = item['total_fragmentos']
        # Aqui o segredo: pega só o tamanho (len) dos bytes
        tamanho = len(item['fragmento_bytes']) 
        
        print(f"{nome:<20} | {pos:<5} | {total:<5} | {tamanho} bytes")
    print("="*50 + "\n")

imprimir_arquivos_do_nodo()
while True:
    try:
        print("\n⏳ Aguardando mensagem...")
        
        conexao, endereco = servidor.accept()
        dados_bytes = conexao.recv(BUFFER_SIZE)
        
        if not dados_bytes:
            conexao.close()
            continue

        mensagem = pickle.loads(dados_bytes)
        comando = mensagem['comando']
        
        print(f"📩 Recebi comando: {comando} no nodo {MEU_ID}")

        # --- LÓGICA 1: UPLOAD (GUARDAR ARQUIVO) ---
        if comando == 'UPLOAD':
            nome_arquivo = mensagem['nome_arquivo']
            print(f"✅ Arquivo '{nome_arquivo}' recebido!")
            porta_proximo_node = get_porta_proximo_node()
            conteudo_bytes = mensagem['conteudo_bytes']
            total_fragmentos = (len(conteudo_bytes) + FRAG_SIZE - 1) // FRAG_SIZE

            try:
                for i in range(total_fragmentos):
                    
                    fragmento = conteudo_bytes[i*FRAG_SIZE : (i+1)*FRAG_SIZE]

                    # salva os fragmentos das posições 0 e 1 no nó que recebeu
                    if i < 1 or porta_proximo_node == None:
                        MEUS_ARQUIVOS.append({
                            "nome_arquivo": nome_arquivo,
                            "posicao": i,
                            "fragmento_bytes": fragmento,
                            "total_fragmentos": total_fragmentos
                        })

                    # manda replicar, caso seja o fragmento 0 ou 1, informa que já foi salvo em 1 node
                    if porta_proximo_node:
                        frag_data = {
                            "comando": "REPLICAR",
                            "nome_arquivo": nome_arquivo,
                            "posicao": i,
                            "total_fragmentos": total_fragmentos,
                            "fragmento_bytes": conteudo_bytes[i*FRAG_SIZE : (i+1)*FRAG_SIZE],
                            "qnt_nodes_que_salvaram": 1 if i < 1 else 0
                        }

                        enviar_para_node(porta_proximo_node, frag_data)
                        print(f"➡️ Cópia enviada para vizinho {porta_proximo_node}")
                    else:
                        print(f"➡️ Fragmento {i} salvo localmente pois não foi encontrado outro nodo")

                imprimir_arquivos_do_nodo()
                conexao.sendall(pickle.dumps(resposta))
            except:
                print(f"⚠️ Não consegui enviar cópia para {porta_proximo_node} (ele está offline?)")

        # --- LÓGICA 2: REPLICAR ---
        elif comando == 'REPLICAR':
            nome_arquivo = mensagem['nome_arquivo']
            print(f"✅ Arquivo '{nome_arquivo}' posicao {posicao} recebido!")

            posicao = mensagem['posicao']
            fragmento_bytes = mensagem['fragmento_bytes']
            total_fragmentos = mensagem['total_fragmentos']
            qnt_nodes_que_salvaram = mensagem['qnt_nodes_que_salvaram']

            # Se 2 nodes tenham salvo o fragmento, então o node não irá salvar
            if qnt_nodes_que_salvaram == 2:
                print("2 nodes já salvaram este fragmento, então decidi não salvar")
                continue

            porta_proximo_node = get_porta_proximo_node()

            qtd = len([arq for arq in MEUS_ARQUIVOS if arq["nome_arquivo"] == nome_arquivo])

            try:
                if qtd < 2 :
                    print("Tem menos de 2 fragmentos do arquivo")

                    MEUS_ARQUIVOS.append({
                        "nome_arquivo": nome_arquivo,
                        "posicao": posicao,
                        "fragmento_bytes": fragmento_bytes,
                        "total_fragmentos": total_fragmentos
                    })
                    
                    qnt_nodes_que_salvaram = qnt_nodes_que_salvaram + 1

                # Se menos 0 ou 1 node salvou o fragmento, será repassado par ao proximo node salvar
                if qnt_nodes_que_salvaram < 2:
                    frag_data = {
                        "comando": "REPLICAR",
                        "nome_arquivo": nome_arquivo,
                        "posicao": posicao,
                        "total_fragmentos": total_fragmentos,
                        "fragmento_bytes": fragmento_bytes,
                        "qnt_nodes_que_salvaram": qnt_nodes_que_salvaram
                    }

                    enviar_para_node(porta_proximo_node, frag_data)
                    print(f"➡️ Cópia enviada para vizinho {porta_proximo_node}")
            except:
                print(f"⚠️ Não consegui enviar cópia para {porta_proximo_node} (ele está offline?)")

        # --- LÓGICA 3: DOWNLOAD (BUSCAR ARQUIVO) ---
        elif comando == 'DOWNLOAD':
            porta_proximo_node = get_porta_proximo_node()
            nome_arquivo = mensagem['nome_arquivo']

            # 1. Coleta o que tem localmente (Isso estava certo)
            fragmentos = []
            for item in MEUS_ARQUIVOS:
                if item['nome_arquivo'] == nome_arquivo:
                    fragmentos.append({
                        'posicao': item['posicao'],
                        'data': item['fragmento_bytes'],
                        'total': item['total_fragmentos']
                    })

            # --- CORREÇÃO 1: Lógica segura para saber se precisa buscar ---
            precisa_buscar = False
            if not fragmentos:
                precisa_buscar = True # Se não tenho nada, busco
            elif len(fragmentos) < fragmentos[0]['total']:
                precisa_buscar = True # Se tenho incompleto, busco

            # 2. Busca no anel se necessário
            if precisa_buscar and porta_proximo_node:
                print(f"🔍 Buscando partes faltantes de '{nome_arquivo}' no anel...")
                req = {
                    "comando": "BUSCAR_ANEL", # Use um nome consistente (RECUPERAR ou BUSCAR)
                    "nome_arquivo": nome_arquivo,
                    "origem": MINHA_PORTA,
                    "fragments": fragmentos # Envia o que já tenho
                }

                try:
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.connect((MEU_IP, porta_proximo_node))
                    s.sendall(pickle.dumps(req))
                    
                    resp_bytes = b""
                    while True:
                        chunk = s.recv(4096)
                        if not chunk: break
                        resp_bytes += chunk
                    s.close()
                    
                    # --- CORREÇÃO 3: Pega a chave certa da resposta ---
                    # A resposta deve conter a lista 'fragments' atualizada pelos outros nós
                    resposta_anel = pickle.loads(resp_bytes)
                    fragmentos = resposta_anel.get('fragments', fragmentos)
                    
                except Exception as e:
                    print(f"Erro ao buscar no anel: {e}")

            # --- CORREÇÃO 4: Validação final ---
            if not fragmentos:
                resposta = {"status": "ERRO", "msg": "Arquivo não encontrado."}
                conexao.sendall(pickle.dumps(resposta))
                # continue ou return aqui
            
            # --- CORREÇÃO 2: ORDENAÇÃO OBRIGATÓRIA ---
            # Garante que a posição 0 vem antes da 1, etc.
            fragmentos.sort(key=lambda x: x['posicao'])

            # Verifica integridade
            total_esperado = fragmentos[0]['total']
            if len(fragmentos) != total_esperado:
                print(f"⚠️ Arquivo incompleto: Tenho {len(fragmentos)} de {total_esperado}")
                resposta = {"status": "ERRO", "msg": "Arquivo incompleto."}
            else:
                # Junta tudo
                conteudo_final = b''.join(f['data'] for f in fragmentos)
                
                resposta = {
                    'status': 'OK',
                    'nome_arquivo': nome_arquivo,
                    'conteudo_bytes': conteudo_final
                }

            conexao.sendall(pickle.dumps(resposta))
            
        elif comando == 'RECUPERAR_FRAGMENTO':
            print(f"Recuperando fragmento")

        elif comando == 'LISTAR_FRAGMENTOS':
            # print(MEUS_ARQUIVOS)
            imprimir_arquivos_do_nodo()

        conexao.close()

    except KeyboardInterrupt:
        print("\nDesligando o nó...")
        break
    except Exception as e:
        print(f"Deu um erro: {e}")
        conexao.sendall(pickle.dumps('deu erro'))

servidor.close()