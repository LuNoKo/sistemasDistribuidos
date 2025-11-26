import socket
import pickle
import sys

# --- CONFIGURAÇÕES ---
# Topologia: A -> B -> C -> D -> A
QUEM_E_O_PROXIMO = {'A': 'B', 'B': 'C', 'C': 'D', 'D': 'A'}
PORTAS = {'A': 5001, 'B': 5002, 'C': 5003, 'D': 5004}
MEU_IP = '127.0.0.1'
BUFFER_SIZE = 4096
FRAG_SIZE = 64 # Tamanho pequeno para forçar a fragmentação e testar

# --- ESTADO ---
MEUS_ARQUIVOS = [] # Lista de dicionários

# --- INICIALIZAÇÃO ---
if len(sys.argv) < 2:
    print("Use: python Node.py <ID> (Ex: python Node.py A)")
    sys.exit()

MEU_ID = sys.argv[1].upper()
MINHA_PORTA = PORTAS.get(MEU_ID, 5000)
PROXIMO_ID = QUEM_E_O_PROXIMO.get(MEU_ID, 'A')
PORTA_DO_PROXIMO = PORTAS.get(PROXIMO_ID, 5000)

print(f"--- NÓ {MEU_ID} RODANDO NA PORTA {MINHA_PORTA} ---")
# print(f"➡️ Vizinho: {PROXIMO_ID} ({PORTA_DO_PROXIMO})")

servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
servidor.bind((MEU_IP, MINHA_PORTA))
servidor.listen(5)

def get_porta_proximo_node(portas_para_ignorar=[]):
    """Retorna a porta do próximo nó ativo."""
    proximo_id = QUEM_E_O_PROXIMO[MEU_ID]
    # Tenta conectar no sucessor imediato. Se falhar, pula para o próximo (lógica simplificada)
    for _ in range(len(PORTAS)):
        porta_candidata = PORTAS[proximo_id]

        # 1. VERIFICA SE DEVE IGNORAR (Correção: usar 'in' e não '==')
        if porta_candidata in portas_para_ignorar:
            # Pula para o próximo ID sem nem tentar conectar
            proximo_id = QUEM_E_O_PROXIMO[proximo_id]
            continue

        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(0.5)
            s.connect((MEU_IP, porta_candidata))
            s.close()
            return porta_candidata
        except:
            proximo_id = QUEM_E_O_PROXIMO[proximo_id]
    print("Nenhum outro nó está online...")
    return porta_candidata


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
            if len(chunk) < BUFFER_SIZE: break # Break simples se o pacote for pequeno
        
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
                
                # Salva na memória
                # MEUS_ARQUIVOS.append({
                #     "nome_arquivo": nome,
                #     "posicao": i,
                #     "fragmento_bytes": pedaco,
                #     "total_fragmentos": total_frags
                # })
                
                # Prepara lista para mandar pro vizinho depois
                fragmentos_para_enviar.append({
                    "comando": "REPLICAR",
                    "nome_arquivo": nome,
                    "posicao": i,
                    "fragmento_bytes": pedaco,
                    "total_fragmentos": total_frags,
                    "qnt_nodes_que_salvaram": 1 # Eu sou o 1º
                })

            # 2. RESPOSTA IMEDIATA AO CLIENTE (FIM DO LOOPING)
            # Avisa que deu certo e fecha a conexão com o cliente
            resp = {"status": "OK", "msg": f"Nó {MEU_ID} salvou o arquivo."}
            conexao.sendall(pickle.dumps(resp))
            conexao.close() # <--- O SEGREDO ESTÁ AQUI. Libera o cliente.
            
            print("✅ Cliente liberado. Iniciando replicação em background...")

            # 3. REPLICAÇÃO PARA O VIZINHO (Sem travar o cliente)
            porta_proximo_node = MINHA_PORTA
            portas_para_ignorar = []
            try:
                for frag in fragmentos_para_enviar:
                    for i in range(2):
                        if porta_proximo_node == MINHA_PORTA: 
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

                        if len(portas_para_ignorar) != 3:
                            portas_para_ignorar.append(porta_proximo_node)
                        else:
                            portas_para_ignorar = []

                        porta_proximo_node = get_porta_proximo_node(portas_para_ignorar)
                print(f"➡️ Todos os fragmentos replicados para {PROXIMO_ID}.")
            except Exception as e:
                print(f"⚠️ Erro ao replicar para vizinho: {e}")
            
            continue 

        # ---------------------------------------------------------
        # LÓGICA DE REPLICAR (Recebe do Vizinho)
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
                
                # Repassa para o próximo (Incrementando contador)
                try:
                    mensagem['qnt_nodes_que_salvaram'] += 1
                    s_vizinho = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s_vizinho.connect((MEU_IP, PORTA_DO_PROXIMO))
                    s_vizinho.sendall(pickle.dumps(mensagem))
                    s_vizinho.close()
                    print(f"➡️ Repassado para {PROXIMO_ID}.")
                except:
                    print(f"❌ Falha ao repassar réplica.")
            else:
                print(f"⚠️ Réplica duplicada ignorada.")

            conexao.close()

        # ---------------------------------------------------------
        # LÓGICA DE DOWNLOAD
        # ---------------------------------------------------------
        elif comando == 'DOWNLOAD':

            portas_para_ignorar = [MINHA_PORTA]
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

            # --- Lógica segura para saber se precisa buscar ---
            precisa_buscar = False
            if not fragmentos:
                precisa_buscar = True # Se não tenho nada, busco
            elif len(fragmentos) < fragmentos[0]['total']:
                precisa_buscar = True # Se tenho incompleto, busco

            # 2. Busca no anel se necessário
            while precisa_buscar:
                porta_proximo_node = get_porta_proximo_node(portas_para_ignorar)
                print(f"🔍 Buscando partes faltantes de '{nome_arquivo}' no anel na porta {porta_proximo_node}...")

                req = {
                    "comando": "BUSCAR_ANEL", # Use um nome consistente (RECUPERAR ou BUSCAR)
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
                    
                    # --- CORREÇÃO 3: Pega a chave certa da resposta ---
                    # A resposta deve conter a lista 'fragments' atualizada pelos outros nós
                    resposta_anel = pickle.loads(resp_bytes)
                    fragmentos = list({item['posicao']: item for item in fragmentos + resposta_anel.get('fragments', fragmentos)}.values())

                except Exception as e:
                    print(f"Erro ao buscar no anel: {e}")

                if len(portas_para_ignorar) != 3:
                    portas_para_ignorar.append(porta_proximo_node)
                else:
                    precisa_buscar = False

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
        
        elif comando == 'BUSCAR_ANEL':
            nome = mensagem['nome_arquivo']
            print(f"🔍 Recebi pedido de busca para '{nome}'. Verificando memória...")

            # 1. Filtra os fragmentos que ESTE NÓ possui
            # Precisamos converter o formato da memória (fragmento_bytes) 
            # para o formato que o cliente espera (data)
            fragmentos_encontrados = []
            
            for item in MEUS_ARQUIVOS:
                if item['nome_arquivo'] == nome:
                    fragmentos_encontrados.append({
                        'posicao': item['posicao'],
                        'data': item['fragmento_bytes'],  # Cliente espera 'data'
                        'total': item['total_fragmentos'] # Cliente espera 'total'
                    })

            print(f"   ✅ Encontrei {len(fragmentos_encontrados)} fragmentos locais.")

            # 2. Monta a resposta
            # A chave DEVE ser 'fragments' pois seu cliente usa .get('fragments')
            resposta = {
                "status": "OK",
                "fragments": fragmentos_encontrados
            }

            # 3. Envia e encerra
            conexao.sendall(pickle.dumps(resposta))
            conexao.close()

        # ---------------------------------------------------------
        # LÓGICA DE LISTAGEM (DEBUG)
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
        break
    except Exception as e:
        print(f"Erro fatal no loop: {e}")
        # Tenta fechar conexão se der erro pra não travar cliente
        try: conexao.close() 
        except: pass

servidor.close()