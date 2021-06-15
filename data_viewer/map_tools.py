import ezdxf
import pandas as pd
from io import StringIO

def dxf_to_df_lines(file):
    doc = ezdxf.readfile(file)
    msp = doc.modelspace()
    lines = []
    for e in msp:
        line = [e.dxf.layer, e.dxftype()]
        if e.dxftype() == "LINE":
            lines.append(list(e.dxf.start[:2]) + list(e.dxf.end[:2]) + line)
        elif e.dxftype() == "LWPOLYLINE":
            for i in range(len(e)-1):
                lines.append(list(e[i][:2]) + list(e[i+1][:2]) + line)
                if sum(e[i][2:])!=0.0:
                    print('LWPolyline start width, end width, bulge not supported, hence ignored. ', e[i])
        else:
            print('Dxftype {} not supported, hence ignored'.format(e.dxftype()))
    return pd.DataFrame(lines, columns=['x1', 'y1', 'x2', 'y2', 'layer', 'dxftype'])

def dxf_to_dict(file):
    dxf_dict = {}
    doc = ezdxf.readfile(file)
    msp = doc.modelspace()
    for e in msp:
        if e.dxftype() == "LINE":
            points = [e.dxf.start[:2], e.dxf.end[:2]]
        elif e.dxftype() == "LWPOLYLINE":
            points = e.get_points('xy')
        else:
            print('Dxftype {} not supported, hence ignored'.format(e.dxftype()))

        if e.dxf.layer in dxf_dict:
            if e.dxftype() in dxf_dict[e.dxf.layer]:
                dxf_dict[e.dxf.layer][e.dxftype()].append(points)
            else:
                dxf_dict[e.dxf.layer][e.dxftype()] = [points]
        else:
            dxf_dict[e.dxf.layer] = {e.dxftype(): [points]}
    return dxf_dict

def reduced_flightplan(file, n):
    with open(file, 'r') as f:
        lines_in = f.readlines()
    out = ''
    for line in lines_in:
        out += ','.join(line.split(';')[:n])+'\n'
    
    return StringIO(out)