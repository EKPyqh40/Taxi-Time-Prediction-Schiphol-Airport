# Import modules
from pathlib import Path
import pygame
import pandas as pd
import numpy as np
import glob
from map_tools import dxf_to_dict, reduced_flightplan
import time
from math import cos, pi
import json
import os
import sys
import struct

# Read Settings
os.chdir(Path(__file__).absolute().parent)
with open("settings.json", "r") as f:
    settings = json.load(f)

# Inconsistent WIP Legacy:
COLORS = [settings["colors"]["dark_grey"],
    settings["colors"]["red"],
    settings["colors"]["green"],
    settings["colors"]["blue"],
    settings["colors"]["yellow"],
    settings["colors"]["pink"],
    settings["colors"]["bluegreen"],
    settings["colors"]["black"],
]
settings["screen"]["size"] = (settings["screen"]["width"], settings["screen"]["height"])

# Init pygame
pygame.init()
screen = pygame.display.set_mode(settings["screen"]["size"])
font = pygame.font.Font(None, settings["screen"]["fontsize"])

# Generate layers: 
# all polygons in the polygon folder are a single layer
# each layer of the dxf files are the other layers 

# Read polygons
polygons = {}
for path in glob.iglob(settings["paths"]["polygons"] + "*.json"):
    with open(path) as f:
        polygons.update(json.load(f))


# Extremely Ugly retrofit to be able to handle multiple polygons with the same name in a single json list
# Please Improve :D
layers_data = list(polygons.values())
unpacked = False
while not unpacked:
    unpacked = True
    n_removed = 0
    for j, layer in enumerate(layers_data):
        if type(layer[0][0])==list:
            unpacked = False
            for k in layer:
                layers_data.append(k)
            layers_data.pop(j-n_removed)
            n_removed += 1

layers_data = {'polygons': {
    "LWPOLYLINE CLOSED": layers_data
}}

for path in glob.iglob(settings["paths"]["dxf"] + "*.dxf"): # alt: with open("data/layers.json") as f: l = json.load(f)
    layers_data.update(dxf_to_dict(path))
layers_name = np.array(list(layers_data))
layers_on = np.array([False]*len(layers_name))
for layer in settings["screen"]["maps_on"]:
    layers_on[np.where(layers_name==layer)] = True
layers_d = True

# for i, polygon in enumerate(layers_data['Twrsysrules Rode lijnen']['LWPOLYLINE']):
#     polygons['Ramp {:0>2d}'.format(i)] = polygon

# # Part of a test to improve zoomed in performance
# import pandas as pd
# test = []
# for layer in layers_data:
#     for layer_type in layers_data[layer]:
#         for line in layers_data[layer][layer_type]:
#             if layer_type == "LINE":
#                 test.append([layer, line[0][0], line[0][1], line[1][0], line[1][1]])
#             elif layer_type == "LWPOLYLINE":
#                 for point in range(len(line)-1):
#                     test.append([layer, line[point][0], line[point][1], line[point+1][0], line[point+1][1],])
#             elif layer_type == "LWPOLYLINE CLOSED":
#                 for point in range(len(line)):
#                     if point + 1 == len(line):
#                         test.append([layer, line[point][0], line[point][1], line[0][0], line[0][1]])
#                     else:
#                         test.append([layer, line[point][0], line[point][1], line[point+1][0], line[point+1][1],])
#             else:
#                 print("Layer type '{}' not supported".format(layer_type))
# test = pd.DataFrame(data=test, columns=["layer", "x1", "y1", "x2", "y2"])

# bitmap background layer (lvnl aerodrome chart)
bmap_data = pygame.image.load(settings["paths"]["bmap_file"]).convert()
with open(settings["paths"]["bmap_docs"], "r") as f:
    bmap_docs = json.load(f)

# read astra
with open(settings["paths"]["astra_docs"], "r") as f:
    ast_docs = json.load(f)
ast_data = pd.read_csv(settings["paths"]["astra_file"], delimiter=";", names=ast_docs["names"], dtype=ast_docs["dtype"])
ast_data[~ast_data['f_id'].isna()]

# fp_data = pd.read_csv(
#     reduced_flightplan('Data/flightplan.fp', len(fp_columns)), 
#     names=fp_columns)
# fp_data = fp_data[["acid", "adep", "dest", "a_c_type", "l_rwy_id", "to_rwy_id"]]

# Intialize program
t_on = True
t = min(ast_data['t']) + settings["simulation"]["t_start"]

x, y, z, dx, dy, dz = 0, 0, 0, 0, 0, 0
ppm = .1 * (2**.5)**z

# Generate aircraft sprite (dot)
ac_surface = pygame.Surface((10,10))
ac_surface.fill(settings["colors"]["pink"])
ac_surface.set_colorkey(settings["colors"]["pink"])
pygame.draw.circle(ac_surface, settings["colors"]["black"], (5,5), 5)


time_drawing_ac = 0
#################################
# Coordinate Selection Feature: #
#################################
coordinates = {}

polygon_coordinates = []
for polygon in layers_data['polygons']['LWPOLYLINE CLOSED']:
    polygon_coordinates += polygon
polygon_coordinates = np.array(polygon_coordinates)

map_coordinates = []
for i in layers_data:
    if i!="polygons":
        for j in layers_data[i]:
            for k in layers_data[i][j]:
                map_coordinates += np.array(k).tolist()
map_coordinates = np.array(map_coordinates)
###################################

test_on = False

i, fps = 0, 0
speed_on, speed, pos, pos_d, text_tr, text_br, text_tl, cache = True, 1, False, False,  [], [], [], {}
ast_obj_sel = False
bmap_d, layers_d = True, True
ast_data_t = pd.DataFrame()
time_o = fps_o_t = time.time()
fps_o_i = 0
rect_o = rect_u = []
running = True
bmap_on = settings["screen"]["bmap_on"]
while running:
    # Checking Events
    for event in pygame.event.get():
        if event.type == pygame.QUIT: 
            running = False
        if event.type == pygame.KEYDOWN:
            if pygame.key.name(event.key) in np.arange(10).astype(str):
                layer_nr = (int(pygame.key.name(event.key))-1)%len(layers_on)
                layers_on[layer_nr] = not layers_on[layer_nr]
                layers_d = True
                print(layers_name[layer_nr])
            elif pygame.key.name(event.key) == '`':
                bmap_on = not bmap_on
                bmap_d = True
            elif pygame.key.name(event.key) == '[+]' or event.key == pygame.K_q:
                dz = 1
            elif pygame.key.name(event.key) == '[-]' or event.key == pygame.K_e:
                dz = -1
            elif event.key in [pygame.K_LEFT, pygame.K_a]:
                dx = -1 
            elif event.key in [pygame.K_RIGHT, pygame.K_d]:
                dx = 1 
            elif event.key in [pygame.K_UP, pygame.K_w]:
                dy = 1
            elif event.key in [pygame.K_DOWN, pygame.K_s]:
                dy = -1
            elif event.key == pygame.K_r:
                speed *= 2
            elif event.key == pygame.K_f:
                speed /= 2
            elif event.key == pygame.K_SPACE:
                speed_on = not speed_on
            elif event.key == pygame.K_z:
                speed *= -1
            elif event.key == pygame.K_t:
                test_on = not test_on
                layers_d = True
        if event.type == pygame.KEYUP:
            if event.key in [pygame.K_LEFT, pygame.K_RIGHT, pygame.K_a, pygame.K_d]:
                dx = 0
            elif event.key in [pygame.K_UP, pygame.K_DOWN, pygame.K_w, pygame.K_s]:
                dy = 0
            elif pygame.key.name(event.key) in ['[-]','[+]'] or event.key in [pygame.K_q, pygame.K_e]:
                dz = 0
        if event.type == pygame.MOUSEBUTTONDOWN:
            if event.button == 1:
                pos_d = True
                pos = event.pos
            elif event.button == 3:
                pos = False
    
    # Calculations
    if speed_on:
        t_step = int( (time.time() - time_o) * speed )
        t += t_step
        if t_step:
            time_o = time.time()
    else:
        time_o = time.time()
    
    if time.time()-fps_o_t >= 1:
        fps = (i-fps_o_i) / (time.time() - fps_o_t) 
        fps_o_t, fps_o_i = time.time(), i
        # print(len(test))

    if dx or dy or dz:
        x += dx * 5 / ppm
        y += dy * 5 / ppm
        z += dz * 0.1
        if dz:
            ppm = .1 * (2**.5)**z
        bmap_d, layers_d = True, True
        text_bl = ["mpp {:06.2f}".format(1/ppm), "z {}".format(z), "y {}".format(y), "x {}".format(x)]

    if bmap_on and bmap_d:
        corner = [(x-settings["screen"]["width"]/2/ppm)*bmap_docs["ppm"]+bmap_docs["tower"][0],
            (-y-settings["screen"]["height"]/2/ppm)*bmap_docs["ppm"]+bmap_docs["tower"][1]]
        bmap = pygame.Surface((settings["screen"]["width"]/ppm*bmap_docs["ppm"], settings["screen"]["height"]/ppm*bmap_docs["ppm"]))
        bmap.fill(settings["colors"]["white"])
        bmap.blit(bmap_data, (0,0), (corner[0],corner[1],bmap.get_width(), bmap.get_height()))
        bmap = pygame.transform.scale(bmap, settings["screen"]["size"])
    
    if layers_d:
        # if not test_on:
        layers_lines = []
        for layer in layers_name[layers_on]:
            line_color = COLORS[np.argwhere(layers_name==layer)[0][0]%len(COLORS)]
            for line_type in layers_data[layer].keys():
                for line_data in layers_data[layer][line_type]:
                    line_data = np.add(
                        np.add(
                            np.multiply(np.array(line_data), (ppm, -ppm)), 
                            (settings["screen"]["width"]/2, settings["screen"]["height"]/2)), 
                        (-x*ppm,y*ppm))
                    layers_lines.append( {
                        "type": line_type,
                        "color": line_color,
                        "data": line_data
                    })
        # else:
        #     x_lim = (x-1.05*settings["screen"]["width"]/2/ppm, x+1.05*settings["screen"]["width"]/2/ppm)
        #     y_lim = (y-1.05*settings["screen"]["height"]/2/ppm, y+1.05*settings["screen"]["height"]/2/ppm)
        #     layers_lines = test[test["layer"].isin(layers_name[layers_on])].copy()
        #     layers_lines = layers_lines[
        #         ((layers_lines[["x1", "x2"]] >= x_lim[0]) & (layers_lines[["x1", "x2"]] <= x_lim[1])).any(axis=1) &
        #         ((layers_lines[["y1", "y2"]] >= y_lim[0]) & (layers_lines[["y1", "y2"]] <= y_lim[1])).any(axis=1)
        #     ]
        #     layers_lines [["x1", "x2"]] = layers_lines[["x1", "x2"]]*ppm + settings["screen"]["width"]/2 - x*ppm
        #     layers_lines[["y1", "y2"]] = layers_lines[["y1", "y2"]]*-ppm + settings["screen"]["height"]/2 + y*ppm


    
    if t_step or bmap_d: # Only when tracking ac (currently always)
        ast_data_t = ast_data.loc[ast_data['t']==t,:].copy()
        ast_data_t['x_screen'] = ((ast_data_t[['x']] - x)*ppm + settings["screen"]["width"]/2).astype('int64')
        ast_data_t['y_screen'] = ((ast_data_t[['y']] - y)*-ppm +settings["screen"]["height"]/2).astype('int64')
    

    text_tl = [time.strftime('%H:%M:%S',  time.gmtime(t)), 
        'Speed: ' + str(speed*speed_on), 
        'Step: {}'.format(max([1, t_step])), 
        "Fps: {:06.2f}".format(fps),
        "Test: {}".format(test_on)]
    if pos_d:
        x_pos, y_pos = (pos[0]-settings["screen"]["width"]/2)/ppm + x, (pos[1]-settings["screen"]["height"]/2)/-ppm + y 
        text_br = [str(pos), "({},{})".format(x_pos, y_pos)]

        # Coordinates Selection Feature
        print("Nearest Manual Polygon Point: {}".format(polygon_coordinates[np.argmin(np.linalg.norm(polygon_coordinates-[x_pos, y_pos], axis=1))]))
        print("Nearest Map Point: {}".format(map_coordinates[np.argmin(np.linalg.norm(map_coordinates-[x_pos, y_pos], axis=1))]))
        print([x_pos , y_pos])
        # pos_name = input("Pos Name: ")
        # if pos_name != '':
        #     coordinates[pos_name] = [x_pos, y_pos]

        distances = np.linalg.norm(ast_data_t[['x_screen', 'y_screen']].sub(np.array(pos)), axis=1)
        if np.min(distances) <3:
            ast_obj_sel = ast_data_t.iloc[np.argmin(distances)]['trk']
        else:
            ast_obj_sel = False

    if pos and ast_obj_sel and (t_step or pos_d):
        ast_obj_data = ast_data_t[ast_data_t['trk']==ast_obj_sel]
        if len(ast_obj_data) == 0:
            text_tr = ["Aircraft lost"]
        else:
            if len(ast_obj_data) > 1:
                print("Multiple ac with trk {}".format(ast_obj_sel))
            ast_obj_data = ast_obj_data.iloc[0] # Test for more than one result?ast
            text_tr = str(ast_obj_data[['trk', 'fl', 'gs', 'heading', 'f_id', 'mode-s', 'x', 'y']]).split('\n')[:-1]
            text_tr += str(ast_obj_data[12:][ast_obj_data[12:].values==True]).split('\n')[:-1]
            # for fp_i, fp_data_ac in fp_data[fp_data['acid']==ast_obj_data['f_id']].iterrows():
            #     text_tr.append("---Flight Plan Data {}---".format(fp_i+1))
            #     text_tr += str(fp_data_ac).split('\n')[:-1]

    if not pos:
        text_tr = text_br = []

    text_bl = [str(round(x,2)), str(round(y,2)), str(round(z,2)), str(round(1/ppm,2)) ][::-1]

    # Drawing    
    screen.fill(settings["colors"]["white"])

    if bmap_on:
        screen.blit(bmap, (0,0))

    # if not test_on:
    for line in layers_lines:
        if line['type'] == "LINE":
            pygame.draw.line(screen, line['color'], line['data'][0], line['data'][1], 3)
        elif line['type'] == "LWPOLYLINE":
            pygame.draw.lines(screen, line['color'], False, line['data'], 3)
        elif line['type'] == "LWPOLYLINE CLOSED":
            pygame.draw.lines(screen, line['color'], True, line['data'], 3)
        else:
            print("Draw type '{}' not supported".format(line['type']))
    # else:
    #     for bob, k in layers_lines.iterrows():
    #         pygame.draw.line(screen, (0,0,0), (k["x1"], k["y1"]), (k["x2"], k["y2"]), 3)
            

    
    for x_ac, y_ac in ast_data_t.values[:, -2:]:
        rect_u.append(pygame.draw.circle(screen, settings["colors"]["black"], (x_ac, y_ac), 5))
        # Should really only update ac at t_step, but then you need to remember last position as well

        # if pos_d and pos:
        #     distances = np.linalg.norm(ast_data_t[['x_screen', 'y_screen']].sub(np.array(pos)), axis=1)
        #     if np.min(distances) > 3:
        #         text_tr = ["Click Closer"]
        #     else:
        #         ast_data_ac = ast_data_t.iloc[np.argmin(distances)]
        #         fp_data_acs = fp_data[fp_data['acid']==ast_data_ac['f_id']] # Could be multiple?
        #         text_tr =str(ast_data_ac[['trk', 'ssr', 'f_id', 'mode-s']]).split('\n')[:-1]
        #         for ac_i, fp_data_ac in fp_data_acs.iterrows():
        #             text_tr.append("---Flight Plan Data {}---".format(ac_i+1))
        #             text_tr += str(fp_data_ac[['acid', 'adep', 'dest', 'a_c_type']]).split('\n')[:-1]
        #     #pos_d = False
        #     print(text_tr)

    if layers_on[np.where(layers_name=="polygons")]: # Shades the polygons (outline already done)
        polygon_screen = pygame.Surface(settings["screen"]["size"], pygame.SRCALPHA, 32)
        for polygon in layers_data['polygons']['LWPOLYLINE CLOSED']:
            polygon = np.add(np.add(np.multiply(np.array(polygon), (ppm, -ppm)), (settings["screen"]["width"]/2, settings["screen"]["height"]/2)), (-x*ppm,y*ppm))
            rect_u.append(pygame.draw.polygon(polygon_screen, (100,100,100, 125), polygon))
        screen.blit(polygon_screen, (0,0))

    # polygon_screen = pygame.Surface(size)
    # polygon_screen.fill(PINK)
    # polygon_screen.set_colorkey(PINK)
    # for polygon in polygons:
    #     polygon = np.add(np.add(np.multiply(np.array(polygons[polygon]), (ppm, -ppm)), (settings["screen"]["width"]/2, settings["screen"]["height"]/2)), (-x*ppm,y*ppm))
    #     rect_u.append(pygame.draw.polygon(polygon_screen, (100,100,100), polygon))
    # polygon_screen.set_alpha(128)
    # screen.blit(polygon_screen, (0,0))

    
    # Generate the different text on screen
    for j, line in enumerate(text_tl): # Performance: Only when text_tl changes
        # if not str(line) in cache:
        #     cache[str(line)] = font.render(line, False, settings["colors"]["black"], settings["colors"]["white"])
        line = font.render(line, False, settings["colors"]["black"], settings["colors"]["white"])
        rect_u.append(screen.blit(line, (0, j*settings["screen"]["fontsize"])))
    
    for j, line in enumerate(text_tr): # Performance: only when text_tr changes
        line = font.render(line, False, settings["colors"]["black"], settings["colors"]["white"])
        rect_u.append(screen.blit(line, (settings["screen"]["width"]-line.get_width(), j*settings["screen"]["fontsize"])))
    
    for j, line in enumerate(text_bl):
        line = font.render(line, False, settings["colors"]["black"], settings["colors"]["white"])
        rect_u.append(screen.blit(line, (0, settings["screen"]["height"]-(j+1)*settings["screen"]["fontsize"])))
    
    for j, line in enumerate(text_br):
        line = font.render(line, False, settings["colors"]["black"], settings["colors"]["white"])
        rect_u.append(screen.blit(line, (settings["screen"]["width"]-line.get_width(), settings["screen"]["height"]-(j+1)*settings["screen"]["fontsize"])))

    if layers_d or bmap_d:
        pygame.display.flip()
    else:
        pygame.display.update(rect_u+rect_o)
        rect_o = rect_u

    # Reset Frame
    i += 1
    bmap_d, layers_d, pos_d = False, False, False
    rect_u = []

# # Coordinates Selection Feature
# if input("Store? [0/1]")=="1":
#     with open("coordinates_selection_{}.json".format(time.time()), "w") as f:
#         json.dump(coordinates, f, indent=4)

print("End")


# Code for gates:
    # if gates_on:
    #     gates_data[['x', 'y']] = gates_data[['x', 'y']].multiply((ppm, -ppm)).add(settings["screen"]["width"]/2-x*ppm, settings["screen"]["height"]/2+y*ppm)
    #     for j, gate in gates_data.iterrows():
            
    #         rect_u.append(pygame.draw.circle(screen, LIGHT_GREY, (round(gate.x), round(gate.y)), 3))

    # for j, gate in gates_data.iterrows():
    #     pygame.draw.circle(screen, LIGHT_GREY, (int(gate.x*1852*ppm+settings["screen"]["width"]//2), int(-gate.y*1852*ppm+settings["screen"]["height"]//2)), 3)

# Code for additional detailed bitmap
    # if (dx or dy or dz or bmap2_d) and bmap2_on:
    #     corner = [(x-settings["screen"]["width"]/2/ppm)*bmap2_ppm+bmap2_tower[0], (-y-settings["screen"]["height"]/2/ppm)*bmap2_ppm+bmap2_tower[1]]
    #     bmap2 = pygame.Surface((settings["screen"]["width"]/ppm*bmap2_ppm, settings["screen"]["height"]/ppm*bmap2_ppm))
    #     bmap2.fill(settings["colors"]["white"])
    #     bmap2.blit(bmap2_data, (0,0), (corner[0],corner[1],bmap2.get_width(), bmap2.get_height()))
    #     bmap2 = pygame.transform.scale(bmap2, size)
    #     bmap2_d = False
    #     bmap2.set_colorkey(settings["colors"]["white"])

    # if bmap2_on:
    #     screen.blit(bmap2, (0,0))
