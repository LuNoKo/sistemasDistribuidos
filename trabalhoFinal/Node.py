import socket
import pickle
import sys

# --- CONFIGURAÇÕES ---
QUEM_E_O_PROXIMO = {'A': 'B', 'B': 'C', 'C': 'D', 'D': 'A'}
PORTAS = {'A': 5001, 'B': 5002, 'C': 5003, 'D': 5004}
MEU_IP = '127.0.0.1'
BUFFER_SIZE = 4096
FRAG_SIZE = 64

# --- ESTADO ---
MEUS_ARQUIVOS = []

# --- INICIALIZAÇÃO ---
if len(sys.argv) < 2:
    print("Use: python Node.py <ID> (Ex: python Node.py A)")
    sys.exit()

MEU_ID = sys.argv[1].upper()
MINHA_PORTA = PORTAS.get(MEU_ID, 5000)
PROXIMO_ID = QUEM_E_O_PROXIMO.get(MEU_ID, 'A')
PORTA_DO_PROXIMO = PORTAS.get(PROXIMO_ID, 5000)

print(f"--- NÓ {MEU_ID} RODANDO NA PORTA {MINHA_PORTA} ---")

servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
servidor.bind((MEU_IP, MINHA_PORTA))
servidor.listen(5)

def get_porta_proximo_node(portas_para_ignorar=[]):
    nos_offline_desta_busca = []
    proximo_id = QUEM_E_O_PROXIMO[MEU_ID]
    
    for _ in range(len(PORTAS)):
        porta_candidata = PORTAS[proximo_id]

        if porta_candidata in portas_para_ignorar:
            proximo_id = QUEM_E_O_PROXIMO[proximo_id]
            continue

        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(0.5)
            s.connect((MEU_IP, porta_candidata))
            s.close()
            return porta_candidata, nos_offline_desta_busca
        except:
            nos_offline_desta_busca.append(porta_candidata)
            proximo_id = QUEM_E_O_PROXIMO[proximo_id]
    print("Nenhum outro nó está online...")
    return porta_candidata, nos_offline_desta_busca

# --- LOOP PRINCIPAL ---
while True:
    try:
        print("\n⏳ Aguardando conexões...")
        conexao, endereco = servidor.accept()
        
        # Recebe dados
        dados_buffer = b""
        while True:
            chunk = conexao.recv(BUFFER_SIZE)
            if not chunk: break
            dados_buffer += chunk
            if len(chunk) < BUFFER_SIZE: break
        
        if not dados_buffer:
            conexao.close()
            continue

        try:
            mensagem = pickle.loads(dados_buffer)
        except:
            print("❌ Erro ao decodificar pickle.")
            conexao.close()
            continue

        comando = mensagem.get('comando')
        print(f"📩 Comando recebido: {comando}")

        # ---------------------------------------------------------
        # LÓGICA DE UPLOAD (Recebe do Cliente)
        # ---------------------------------------------------------
        if comando == 'UPLOAD':
            nome = mensagem['nome_arquivo']
            conteudo = mensagem['conteudo_bytes']
            total_frags = (len(conteudo) + FRAG_SIZE - 1) // FRAG_SIZE
            
            print(f"📥 Recebendo '{nome}'. Fragmentando em {total_frags} partes...")

            # 1. Salva localmente
            fragmentos_para_enviar = []
            for i in range(total_frags):
                pedaco = conteudo[i*FRAG_SIZE : (i+1)*FRAG_SIZE]
                
                fragmentos_para_enviar.append({
                    "comando": "REPLICAR",
                    "nome_arquivo": nome,
                    "posicao": i,
                    "fragmento_bytes": pedaco,
                    "total_fragmentos": total_frags,
                    "qnt_nodes_que_salvaram": 1
                })

            resp = {"status": "OK", "msg": f"Nó {MEU_ID} salvou o arquivo."}
            conexao.sendall(pickle.dumps(resp))
            conexao.close()
            
            print("✅ Cliente liberado. Iniciando replicação em background...")

            # REPLICAÇÃO PARA O VIZINHO
            porta_proximo_node = MINHA_PORTA
            portas_para_ignorar = []
            try:
                for frag in fragmentos_para_enviar:
                    for i in range(2):
                        ja_tenho = any(a['nome_arquivo'] == frag['nome_arquivo'] and a['posicao'] == frag['posicao'] for a in MEUS_ARQUIVOS)

                        if porta_proximo_node == MINHA_PORTA and not ja_tenho: 
                            MEUS_ARQUIVOS.append({
                                "nome_arquivo": frag['nome_arquivo'],
                                "posicao": frag['posicao'],
                                "fragmento_bytes": frag['fragmento_bytes'],
                                "total_fragmentos": frag['total_fragmentos']
                            })
                        else:
                            s_vizinho = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            s_vizinho.connect((MEU_IP, porta_proximo_node))
                            s_vizinho.sendall(pickle.dumps(frag))
                            s_vizinho.close()

                        if len(portas_para_ignorar) < len(PORTAS) - 1:
                            portas_para_ignorar.append(porta_proximo_node)
                        else:
                            portas_para_ignorar = []

                        porta_proximo_node, lista_offline = get_porta_proximo_node(portas_para_ignorar)
                        portas_para_ignorar.extend(lista_offline)
                print(f"➡️ Todos os fragmentos replicados para {PROXIMO_ID}.")
            except Exception as e:
                print(f"⚠️ Erro ao replicar para vizinho: {e}")
            
            continue 

        # ---------------------------------------------------------
        # LÓGICA DE REPLICAR
        # ---------------------------------------------------------
        elif comando == 'REPLICAR':
            nome = mensagem['nome_arquivo']
            pos = mensagem['posicao']
            qnt_salvos = mensagem['qnt_nodes_que_salvaram']

            # Verifica parada (Se já está em 2 nós, para)
            if qnt_salvos >= 2:
                print(f"🛑 Ciclo de replicação de '{nome}' (Pos {pos}) encerrado.")
                conexao.close()
                continue

            # Verifica se já tenho para não duplicar
            ja_tenho = any(a['nome_arquivo'] == nome and a['posicao'] == pos for a in MEUS_ARQUIVOS)
            
            if not ja_tenho:
                MEUS_ARQUIVOS.append({
                    "nome_arquivo": nome,
                    "posicao": pos,
                    "fragmento_bytes": mensagem['fragmento_bytes'],
                    "total_fragmentos": mensagem['total_fragmentos']
                })
                print(f"💾 Réplica salva (Pos {pos}).")
                
            else:
                print(f"⚠️ Réplica duplicada ignorada.")

            conexao.close()

        # ---------------------------------------------------------
        # LÓGICA DE DOWNLOAD
        # ---------------------------------------------------------
        elif comando == 'DOWNLOAD':
            portas_para_ignorar = [MINHA_PORTA]
            nome_arquivo = mensagem['nome_arquivo']

            # Coleta o que tem localmente
            fragmentos = []
            for item in MEUS_ARQUIVOS:
                if item['nome_arquivo'] == nome_arquivo:
                    fragmentos.append({
                        'posicao': item['posicao'],
                        'data': item['fragmento_bytes'],
                        'total': item['total_fragmentos']
                    })

            # Verificar se precisa buscar em outros nós
            precisa_buscar = False
            if not fragmentos:
                precisa_buscar = True
            elif len(fragmentos) < fragmentos[0]['total']:
                precisa_buscar = True

            # Busca no anel se necessário
            while precisa_buscar:
                porta_proximo_node, lista_offline = get_porta_proximo_node(portas_para_ignorar)
                portas_para_ignorar.extend(lista_offline)
                print(f"🔍 Buscando partes faltantes de '{nome_arquivo}' no anel na porta {porta_proximo_node}...")

                req = {
                    "comando": "BUSCAR_ANEL",
                    "nome_arquivo": nome_arquivo,
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
                    
                    # Adiciona nos fragmentos
                    resposta_anel = pickle.loads(resp_bytes)
                    fragmentos = list({item['posicao']: item for item in fragmentos + resposta_anel.get('fragments', fragmentos)}.values())

                except Exception as e:
                    print(f"Erro ao buscar no anel: {e}")

                # Solicita para todos do anel os fragmentos do arquivo solicitado
                if len(portas_para_ignorar) < len(PORTAS) - 1:
                    portas_para_ignorar.append(porta_proximo_node)
                else:
                    precisa_buscar = False

                # Para de solicitar se já chegou todos os fragmentos
                if len(fragmentos) < fragmentos[0]['total']:
                    precisa_buscar = False

            if not fragmentos:
                resposta = {"status": "ERRO", "msg": "Arquivo não encontrado."}
                conexao.sendall(pickle.dumps(resposta))

            total_esperado = fragmentos[0]['total']
            if len(fragmentos) != total_esperado:
                print(f"⚠️ Arquivo incompleto: Tenho {len(fragmentos)} de {total_esperado}")
                resposta = {"status": "ERRO", "msg": "Arquivo incompleto."}
            else:
                fragmentos.sort(key=lambda x: x['posicao'])
                conteudo_final = b''.join(f['data'] for f in fragmentos)
                
                resposta = {
                    'status': 'OK',
                    'nome_arquivo': nome_arquivo,
                    'conteudo_bytes': conteudo_final
                }

            conexao.sendall(pickle.dumps(resposta))

        # ---------------------------------------------------------
        # LÓGICA DE BUSCAR NA REDE
        # ---------------------------------------------------------
        elif comando == 'BUSCAR_ANEL':
            nome = mensagem['nome_arquivo']
            print(f"🔍 Recebi pedido de busca para '{nome}'. Verificando memória...")

            fragmentos_encontrados = []
            
            for item in MEUS_ARQUIVOS:
                if item['nome_arquivo'] == nome:
                    fragmentos_encontrados.append({
                        'posicao': item['posicao'],
                        'data': item['fragmento_bytes'],
                        'total': item['total_fragmentos']
                    })

            print(f"✅ Encontrei {len(fragmentos_encontrados)} fragmentos locais.")

            resposta = {
                "status": "OK",
                "fragments": fragmentos_encontrados
            }

            conexao.sendall(pickle.dumps(resposta))
            conexao.close()

        # ---------------------------------------------------------
        # LÓGICA DE LISTAGEM
        # ---------------------------------------------------------
        elif comando == 'LISTAR_FRAGMENTOS':
            print("\n" + "="*14 + f" Fragmentos no nodo {MEU_ID} " + "="*14)
            print(f"{'ARQUIVO':<20} | {'POS':<5} | {'TOTAL':<5} | {'TAMANHO'}")
            print("-" * 50)
            
            if not MEUS_ARQUIVOS:
                print("   (Nenhum arquivo na memória)")
            
            for item in MEUS_ARQUIVOS:
                nome = item['nome_arquivo']
                pos = item['posicao']
                total = item['total_fragmentos']
                
                tamanho = len(item['fragmento_bytes']) 
                
                print(f"{nome:<20} | {pos:<5} | {total:<5} | {tamanho} bytes")
            print("="*50 + "\n")
        conexao.close()

    except KeyboardInterrupt:
        print("\nDesligando nó...")
        try: conexao.close()
        except: pass
        break
    except Exception as e:
        print(f"Erro fatal: {e}")
        try: conexao.close()
        except: pass

servidor.close()