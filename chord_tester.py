#!/usr/bin/python
import random

P = [4, 8, 10, 20, 30]
F = 128

file_objects = []
nodes = []
# Go through and select all the nodes for all the various P-values
for val in P:
	selected_nodes = []
	for i in range(val):
		cur_node = random.randrange(1, 255)
		while(cur_node in selected_nodes):
			cur_node = random.randrange(1, 255)
		selected_nodes.append(cur_node)

	fo = open("output_P" + str(val) + "_1.txt", "wb")
	file_objects.append(str(val))
	nodes.append(selected_nodes)
	for i in range(len(selected_nodes)):
		if(i != (len(selected_nodes) - 1) ):
			fo.write("join " + str(selected_nodes[i]) + "\n")
		else:
			fo.write("join " + str(selected_nodes[i]))
	fo.close()

for cur_nodes, fo in zip(nodes, file_objects):
	fo_2 = open("output_P" + str(fo) + "_2.txt", "wb")
	for i in range(128):
		p = random.choice(cur_nodes)
		k = random.randrange(1, 255)
		if(i != 127):
			fo_2.write("find " + str(p) + " " + str(k) + "\n")
		else:
			fo_2.write("find " + str(p) + " " + str(k))
	fo_2.close()

