import sys
import json
import socket
import threading
import time
from threading import Event

# Configurações globais
PORT = 55151
BUFFER_SIZE = 1024

class Router:
    def __init__(self, address, update_period, startup_file=None):
        self.update_event = Event()  # Sinalizador para atualizações imediatas
        self.address = address
        self.update_period = update_period
        self.routing_table = {}
        self.neighbors = {}
        self.last_update_time = time.time()  # Rastreador do último envio de update
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind((self.address, PORT))
        self.running = True
        if startup_file:
            self.process_startup_file(startup_file)
        # Inicia a thread de envio de atualizações
        threading.Thread(target=self.send_updates, daemon=True).start()
        # Inicia a thread de recepção de mensagens
        threading.Thread(target=self.receive_messages, daemon=True).start()

    def process_startup_file(self, startup_file):
        try:
            with open(startup_file, 'r') as f:
                for line in f:
                    command = line.strip().split()
                    if command[0] == 'add':
                        self.add_neighbor(command[1], int(command[2]))
        except FileNotFoundError:
            print(f"Error:'{startup_file}' not find.")
        except ValueError:
            print("Error: wrong format.")

    def add_neighbor(self, ip, weight):
        self.neighbors[ip] = weight
        self.routing_table[ip] = {'distance': weight, 'next_hop': ip}
        self.update_event.set()  # Notifica a thread de envio

    def del_neighbor(self, ip):
        if ip in self.neighbors:
            del self.neighbors[ip]
            self.routing_table = {k: v for k, v in self.routing_table.items() if v['next_hop'] != ip}
            self.update_event.set()  # Notifica a thread de envio

    def send_updates(self):
        while self.running:
            # Sempre envia updates periodicamente
            if self.update_event.is_set():
                self.update_event.clear()  # Reseta o evento após um envio imediato
            
            # Envia para todos os vizinhos
            for neighbor in self.neighbors:
                update_message = {
                    "type": "update",
                    "source": self.address,
                    "destination": neighbor,
                    "distances": {
                        dest: data['distance'] for dest, data in self.routing_table.items()
                    }
                }
                try:
                    self.socket.sendto(json.dumps(update_message).encode(), (neighbor, PORT))
                except Exception as e:
                    print(f"error during send update to {neighbor}: {e}")
            
            # Pausa entre envios periódicos
            time.sleep(self.update_period)

    def receive_messages(self):
        while self.running:
            try:
                data, addr = self.socket.recvfrom(BUFFER_SIZE)
                message = json.loads(data.decode())
                self.handle_message(message)
            except json.JSONDecodeError:
                print("Error: invalid JSON.")

    def handle_message(self, message):
        if message['type'] == 'update':
            self.handle_update(message)
        elif message['type'] == 'data':
            self.handle_data(message)
        elif message['type'] == 'trace':
            self.handle_trace(message)

   
    def handle_update(self, message):
        source = message['source']
        if source not in self.neighbors:
            return
        updated = False  # Rastreador de mudanças
        for dest, distance in message['distances'].items():
            new_distance = self.neighbors[source] + distance
            if dest not in self.routing_table or new_distance < self.routing_table[dest]['distance']:
                self.routing_table[dest] = {'distance': new_distance, 'next_hop': source}
                updated = True
        if updated:
            self.update_event.set()  # Marca para envio imediato

    def handle_data(self, message):
        if message['destination'] == self.address:
            # Exibe apenas o conteúdo do payload no terminal
            print(f"{message['payload']}")
        else:
            next_hop = self.routing_table.get(message['destination'], {}).get('next_hop')
            if next_hop:
                try:
                    self.socket.sendto(json.dumps(message).encode(), (next_hop, PORT))
                except Exception as e:
                    print(f"Error during send message to {next_hop}: {e}")

    def handle_trace(self, message):
        message['routers'].append(self.address)
        if message['destination'] == self.address:
            response = {
                "type": "data",
                "source": self.address,
                "destination": message['source'],
                "payload": json.dumps(message)
            }
            try:
                self.socket.sendto(json.dumps(response).encode(), (message['source'], PORT))
            except Exception as e:
                print(f"Error during send trace to {message['source']}: {e}")
        else:
            next_hop = self.routing_table.get(message['destination'], {}).get('next_hop')
            if next_hop:
                try:
                    self.socket.sendto(json.dumps(message).encode(), (next_hop, PORT))
                except Exception as e:
                    print(f"Error during send trace to {next_hop}: {e}")

    def stop(self):
        self.running = False
        self.socket.close()

    def command_loop(self):
        while self.running:
            try:
                command = input().strip().split()
                if not command:
                    continue
                if command[0] == 'add':
                    self.add_neighbor(command[1], int(command[2]))
                elif command[0] == 'del':
                    self.del_neighbor(command[1])
                elif command[0] == 'trace':
                    trace_message = {
                        "type": "trace",
                        "source": self.address,
                        "destination": command[1],
                        "routers": [self.address]
                    }
                    next_hop = self.routing_table.get(command[1], {}).get('next_hop')
                    if next_hop:
                        try:
                            self.socket.sendto(json.dumps(trace_message).encode(), (next_hop, PORT))
                        except Exception as e:
                            print(f"Error during send trace to {next_hop}: {e}")
                    else:
                        print(f"Route {command[1]} not find.")
                elif command[0] == 'quit':
                    self.stop()
            except ValueError:
                print("Error: command not find.")
            except Exception as e:
                print(f"Error: command not find {e}")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Uso: ./router.py <adress> <interval> [startup]")
        sys.exit(1)
    try:
        address = sys.argv[1]       
        update_period = sys.argv[2]
        # Tenta converter o intervalo para float
        update_period = float(update_period)
        startup_file = sys.argv[3] if len(sys.argv) > 3 else None
        router = Router(address, update_period, startup_file)
        command_thread = threading.Thread(target=router.command_loop)
        command_thread.start()
        try:
            command_thread.join()  # Espera até que o comando `quit` seja executado
        except KeyboardInterrupt:
            router.stop()
    except ValueError:
        print("Erro: Interval needs be a number.")
        sys.exit(1)
