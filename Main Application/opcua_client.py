from opcua import Client
import time

client = Client("opc.tcp://10.42.0.1:4840")

client.connect()

print("Connected to OPC UA Server")

root = client.get_root_node()

print("Root node is:", root)

print("\nBrowsing Objects:\n")

objects = client.get_objects_node()

for child in objects.get_children():
    print(child)

print("\nReading Variables:\n")

while True:

    try:
        children = objects.get_children()

        for obj in children:

            vars = obj.get_children()

            for v in vars:
                try:
                    print(
                        v,
                        "=",
                        v.get_value()
                    )
                except:
                    pass

        print("----------------------")

        time.sleep(2)

    except KeyboardInterrupt:
        break

client.disconnect()
