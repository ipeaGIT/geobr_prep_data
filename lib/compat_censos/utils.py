import os
import json
import geopandas as gpd
import pandas as pd
import networkx as nx
from shapely import LineString, Polygon, MultiPolygon, distance, intersects, minimum_bounding_radius as min_radius
from shapely.geometry import box
from shapely.wkt import loads, dumps
from time import time
import warnings

warnings.filterwarnings('ignore')


# Códigos URM em SIRGAS 2000
UTMCODES_SIRGAS2000 = {
    '17S':"EPSG:31977",
    '18S':"EPSG:31978",
    '19S':"EPSG:31979",
    '20S':"EPSG:31980",
    '21S':"EPSG:31981",
    '22S':"EPSG:31982",
    '23S':"EPSG:31983",
    '24S':"EPSG:31984",
    '25S':"EPSG:31985",
    '17N':"EPSG:31971",
    '18N':"EPSG:31972",
    '19N':"EPSG:31973",
    '20N':"EPSG:31974",
    '21N':"EPSG:31975",
    '22N':"EPSG:31976",
    '23N':"EPSG:6210",
    '24N':"EPSG:6211"
    }

# Códigos URM em WGS84
UTMCODES_WGS84 = {
    **{f"{zone:02d}N": f"EPSG:{32600 + zone}" for zone in range(1, 61)},
    **{f"{zone:02d}S": f"EPSG:{32700 + zone}" for zone in range(1, 61)}
}

time_report = True

def timer(func):
    def wrapper(*args, **kwargs):
        t1 = time()
        result = func(*args, **kwargs)
        end = time()-t1
        if time_report: print(f"'{func.__qualname__}' executed in {end:.4f}s.")
        return result
    return wrapper

def find_utm_proj(X, Y, geosystem='WGS84'):
    '''
    Identifica projeção local UTM
    '''
    assert geosystem in ['WGS84','SIRGAS2000']
    # Hemisfério
    h = 'N' if Y > 0 else 'S'
    # Fuso
    f = int((X//6) + 31)
    fuse = f'{f:02d}{h}'

    if geosystem=='SIRGAS2000':
        return UTMCODES_SIRGAS2000[fuse]
    else:
        return UTMCODES_WGS84[fuse]

def geomIsResidual(geom, ref_ap_ratio):
    '''
    Avalia se geometria é espúria
    '''
    ap_ratio = geom.area/geom.length
    return (ap_ratio < ref_ap_ratio)

def removeHoles(geom, area_min=1):
    '''
    Remove buracos de uma geometria
    '''
    if isinstance(geom, Polygon):
        geom = MultiPolygon([geom])
    out_polys = []
    for part in geom.geoms:
        interiors = []
        for i in part.interiors:
            p = Polygon(i)
            if p.area > area_min:
                interiors.append(i)
        out_polys.append(Polygon(part.exterior.coords, holes=interiors))
    return MultiPolygon(out_polys) if len(out_polys)>1 else out_polys[0]

@timer
def makeCensusLayer(gpkg_file, id_column, layer=None, filter=[], len_higher_hierarchy=11, is_utm=False, geosys=None):
    '''
    Importa malha de setores e a configura
    '''
    if filter:
        filter_len = len(filter[0])
        assert all([len(i)==filter_len for i in filter])
        
    gdf = gpd.read_file(gpkg_file, layer=layer)[[id_column, 'geometry']]
    gdf = gdf.rename(columns={id_column:'ID'})
    gdf['ID'] = gdf['ID'].astype(str)
    if filter:
        gdf = gdf[gdf['ID'].apply(lambda x: x[:filter_len] in filter)]

    # Remoção de sufixos literais
    gdf['ID'] = gdf['ID'].apply(lambda x: ''.join([i for i in str(x) if i.isdigit()]))

    # Correção das geometrias
    gdf['geometry'] = gdf['geometry'].make_valid()

    # Campo de grupo a partir da estrutura do id
    gdf['GROUP'] = gdf['ID'].apply(lambda x: str(x)[:len_higher_hierarchy])

    # Reprojetar camada
    Y = (gdf.total_bounds[1] + gdf.total_bounds[3])/2
    X = (gdf.total_bounds[0] + gdf.total_bounds[2])/2

    if not is_utm:
        if geosys:
            UTMCRS = find_utm_proj(X, Y, geosys)
        else:
            UTMCRS = find_utm_proj(X, Y)
        gdf = gdf.to_crs(UTMCRS)

    # Calcular vizinhos
    gdf['NEIGHBOR'] = gdf.apply(lambda x: getNeighbors(x, gdf), axis=1)

    # Calcular área
    gdf['AREA'] = gdf.area

    return gdf

def getNeighbors(row, geogrid):
    '''
    Identifica vizinhos de cada geometria de uma malha
    '''
    df_neighbors = geogrid.iloc[geogrid['geometry'].sindex.query(row['geometry'], predicate='intersects')]
    df_neighbors = df_neighbors[df_neighbors['ID'] != row['ID']]
    if 'GROUP' in geogrid.columns:
        group = row['GROUP']
        df_neighbors = df_neighbors.query('GROUP == @group')

    return df_neighbors['ID'].to_list()

def makeNeighborhoodGraph(gdf):
    '''
    Cria grafo de vizinhança para conferência
    '''
    G = nx.Graph()
    # Adicionar nós
    for i, row in gdf[['ID', 'geometry']].iterrows():
        G.add_node(row['ID'], center=row['geometry'].representative_point())
    # Dados das arestas
    dic = gdf[['ID', 'NEIGHBOR']]\
          .explode('NEIGHBOR').dropna()\
          .to_dict(orient='records')
    # Adicionar arestas
    for i in dic:
        if (i['ID'], i['NEIGHBOR']) not in G.edges:
            G.add_edge(i['ID'], i['NEIGHBOR'],
                       geom=LineString([G.nodes[i['ID']]['center'], G.nodes[i['NEIGHBOR']]['center']]))
    return G

def exportNeighborhoodGraph(gdf, gpkg_file, layer=''):
    '''
    Exporta grafo de vizinhança em gpkg
    '''
    G = makeNeighborhoodGraph(gdf)
    df = pd.DataFrame(G.edges.data()).rename(columns={0:'node1', 1:'node2', 2:'geometry'})
    df['geometry'] = df['geometry'].apply(lambda x: x['geom'])
    geograph = gpd.GeoDataFrame(df, geometry='geometry', crs=gdf.crs)
    geograph.to_file(gpkg_file, layer=layer, driver='GPKG')

@timer
def getNeighborhoods(geogrid):
    '''
    Retorna coluna de vizinhos dos setores como dicionário
    '''
    dic = geogrid[['ID', 'NEIGHBOR']].to_dict(orient='records')
    dic = {k['ID']:k['NEIGHBOR'] for k in dic}
    return dic

def getGeometries(geogrid):
    '''
    Obtém lista de geometrias indexadas pelo id dos setores
    '''
    geoms = geogrid[['ID', 'geometry']]
    geoms = geoms[['ID', 'geometry']].to_dict(orient='records')
    geoms = {k['ID']:k['geometry'] for k in geoms}
    return geoms

def evaluateUnionArea(gridUnion, ):
    gdf = gridUnion.query('`A.ID` == `B.ID` & `A.PCT_AREA')

def bboxIntersects(geom1, geom2):
    '''
    Testa interseção de bboxes
    '''
    bbox1 = box(*geom1.bounds)
    bbox2 = box(*geom2.bounds)
    return intersects(bbox1, bbox2)

def format_corresp(a, b):
    if a>1:
        if b>1:
            return 'm:n'
        else:
            return 'n:1'
    else:
        if b>1:
            return '1:n'
        else:
            return '1:1'

class comparability_graph(nx.Graph):
    @timer
    def __init__(self, m1, m2, ap_ratio=0.3):
        '''
        Classe de grafo de compatibilização criado das malhas m1 e m2
        '''
        super().__init__()
        self.ap_ratio = ap_ratio
        self.m1 = m1
        self.m2 = m2
        self.makeGridUnion()
        self.classifyGridChanges()
        self.setNodes()
        

        # Cria atributos
        self.MCA = None
        self.compatTable_A = None
        self.compatTable_B = None
        self.edge_gdf = None
        self.node_gdf = None
        self.threshold = []
        self.buffer = []
        self.assess_df = None
        self.scores = None
        self.assessed = False

    @timer
    def reset(self, ap_ratio=0.3):
        self.clear()
        self.setNodes()
        self.ap_ratio = ap_ratio

        # Reseta atributos
        self.MCA = None
        self.compatTable_A = None
        self.compatTable_B = None
        self.edge_gdf = None
        self.node_gdf = None
        self.threshold = []
        self.buffer = []
        self.assess_df = None
        self.scores = None
        self.assessed = False

    def _classifyIdChanges(self):
        '''
        Classifica mudanças entre duas malhas
        '''
        # Dicionários de relações
        dic_1 = getNeighborhoods(self.m1)
        dic_2 = getNeighborhoods(self.m2)

        # Dicionários de geometrias
        geoms_1 = getGeometries(self.m1)
        geoms_2 = getGeometries(self.m2)

        # Calcular correspondencias externas
        outermatchA = self.gridUnion[self.gridUnion['OUTER_MATCH']]['A.ID'].to_list()
        outermatchB = self.gridUnion[self.gridUnion['OUTER_MATCH']]['B.ID'].to_list()
        outermatch = outermatchA + outermatchB

        # Classificação das mudanças
        class_data = {}
        for k, neighbors in dic_1.items():
            if k not in dic_2: 
                class_data[k] = 'Exclusão'
        for k, neighbors in dic_2.items():
            id_kept = k in dic_1
            if id_kept:
                geometry_out = k in outermatch
            neighbors_out = id_kept and not any([i in dic_1 for i in neighbors])
            distance_out = id_kept and distance(geoms_1[k].centroid, geoms_2[k].centroid) > (min_radius(geoms_1[k])+min_radius(geoms_2[k]))
            
            if id_kept and (distance_out or neighbors_out or geometry_out):
                class_data[k] = 'Desassociação'
            elif id_kept:
                class_data[k] = 'Manutenção'
            elif k not in dic_1 and k in dic_2:
                class_data[k] = 'Criação'
            else:
                class_data[k] = ''
        return class_data

    @timer
    def classifyGridChanges(self):
        '''
        Aplica o classificador nas malhas
        '''
        alteracoes = self._classifyIdChanges()
        self.m1['CLASS'] = self.m1['ID'].apply(lambda x: alteracoes[x])
        self.m2['CLASS'] = self.m2['ID'].apply(lambda x: alteracoes[x])
        self.gridUnion['A.CLASS'] = self.gridUnion['A.ID'].apply(lambda x: alteracoes[x])
        self.gridUnion['B.CLASS'] = self.gridUnion['B.ID'].apply(lambda x: alteracoes[x])

    def setNodes(self):
        '''
        Cria os nós do grafo
        '''
        for _, row in self.m1[['ID', 'GROUP', 'CLASS', 'geometry']].iterrows():
            self.add_node(f"A.{row['ID']}",
                        malha='A',
                        nome=row['ID'],
                        group=row['GROUP'],
                        geom=row['geometry'],
                        center=row['geometry'].representative_point(),
                        classe=row['CLASS'])
        for _, row in self.m2[['ID', 'GROUP', 'CLASS', 'geometry']].iterrows():
            self.add_node(f"B.{row['ID']}",
                        malha='B',
                        nome=row['ID'],
                        group=row['GROUP'],
                        geom=row['geometry'],
                        center=row['geometry'].representative_point(),
                        classe=row['CLASS'])

    def _prepareGridForUnion(self, geogrid, prefix=''):
        '''
        Faz tratamento das geometrias para interseção
        '''
        utmcrs = geogrid.crs
        geogrid = geogrid.rename(columns={k:f'{prefix}.{k}' for k in geogrid.columns})
        geogrid = geogrid.set_geometry(f'{prefix}.geometry', crs=utmcrs)
        # Correção contra bug GEOSException: TopologyException: found non-noded intersection
        geogrid[f'{prefix}.geometry'] = [loads(dumps(geom, rounding_precision=3)) for geom in geogrid[f'{prefix}.geometry']]
        return geogrid

    @timer
    def makeGridUnion(self):
        '''
        Cria grid intersecionado de m1 e m2
        '''
        # Preparação para interseção
        gA = self._prepareGridForUnion(self.m1, 'A')
        gB = self._prepareGridForUnion(self.m2, 'B')

        # Interseção por GROUP (município) para evitar GEOSException em UFs grandes.
        # Fallback: buffer(0) + make_valid se overlay direto falhar.
        groups = sorted(set(gA['A.GROUP'].unique()) | set(gB['B.GROUP'].unique()))
        results = []
        n_fail = 0
        for grp in groups:
            gA_grp = gA[gA['A.GROUP'] == grp]
            gB_grp = gB[gB['B.GROUP'] == grp]
            if len(gA_grp) == 0 or len(gB_grp) == 0:
                continue
            try:
                r = gpd.overlay(gA_grp, gB_grp, how='union')
                results.append(r)
            except Exception:
                try:
                    gA_fix = gA_grp.copy()
                    gB_fix = gB_grp.copy()
                    gA_fix['A.geometry'] = gA_fix['A.geometry'].buffer(0).make_valid()
                    gB_fix['B.geometry'] = gB_fix['B.geometry'].buffer(0).make_valid()
                    r = gpd.overlay(gA_fix, gB_fix, how='union')
                    results.append(r)
                except Exception as e2:
                    n_fail += 1
                    print(f"Warning: overlay failed for GROUP {grp}: {e2}")
        if n_fail > 0:
            print(f"Warning: {n_fail} groups failed overlay (skipped)")
        self.gridUnion = pd.concat(results, ignore_index=True) if results else gpd.GeoDataFrame()
        self.gridUnion = self.gridUnion.dropna(subset=['A.ID', 'B.ID'])
        self.gridUnion['RESIDUAL'] = self.gridUnion['geometry'].apply(lambda x: geomIsResidual(x, self.ap_ratio))
        self.gridUnion['UNION_AREA'] = self.gridUnion.area

        # Indicadores de área de interseção
        area_data_A = (self.gridUnion[~self.gridUnion['RESIDUAL']]
                       .pivot_table(index='A.ID', values='UNION_AREA', aggfunc='sum')
                       .reset_index()
                       .rename(columns={'UNION_AREA':'A.ORIGINAL_AREA'}))
        self.gridUnion = self.gridUnion.merge(area_data_A, on='A.ID', how='left')
        self.gridUnion['A.PCT_AREA'] = self.gridUnion.area/self.gridUnion['A.ORIGINAL_AREA']

        area_data_B = (self.gridUnion[~self.gridUnion['RESIDUAL']]
                       .pivot_table(index='B.ID', values='UNION_AREA', aggfunc='sum')
                       .reset_index()
                       .rename(columns={'UNION_AREA':'B.ORIGINAL_AREA'}))
        self.gridUnion = self.gridUnion.merge(area_data_B, on='B.ID', how='left')
        self.gridUnion['B.PCT_AREA'] = self.gridUnion.area/self.gridUnion['B.ORIGINAL_AREA']

        # Recalcular geometria residual
        self.gridUnion['RESIDUAL'] = self.gridUnion.apply(lambda x: x['RESIDUAL'] or x['A.PCT_AREA']<0.05 or x['B.PCT_AREA']<0.05, axis=1)

        # Avalia correspondência de área entre ids distintos acima de 80%
        self.gridUnion['OUTER_MATCH'] = self.gridUnion.apply(lambda x: x['A.ID'] != x['B.ID'] and (x['A.PCT_AREA'] > 0.8 or x['B.PCT_AREA'] > 0.8), axis=1)

    @timer
    def findMaintenances(self):
        '''
        Adiciona arestas de manutenção
        '''
        for s in self.m2.query('CLASS == "Manutenção"')['ID']:
            self.add_edge(f"A.{s}",
                          f"B.{s}",
                          metodo='Manutenção')

    @timer
    def findSplitings(self, threshold):
        '''
        Adiciona arestas de divisão
        '''
        # Selecionar setores para análise
        intersecao = self.gridUnion.query('`A.CLASS` != "Manutenção" | `B.CLASS` != "Manutenção"')

        # Caso setores desassociados com interseção tenham o mesmo id
        for s in intersecao.query('`A.ID` == `B.ID` & `B.PCT_AREA` >= @threshold')['B.ID']:
            self.add_edge(f"A.{s}", f"B.{s}", metodo='Manutenção desassociada')

        # Registrar divisão (>=threshold da área original de B em um único setor de A)
        for _, row in intersecao.query('RESIDUAL == False & `B.PCT_AREA` >= @threshold').iterrows():
            self.add_edge(f"A.{row['A.ID']}",
                          f"B.{row['B.ID']}",
                          metodo='Divisão')
        self.threshold.append(threshold)
        self.clearGroups()
    
    @timer
    def forceOverlay(self, buffer, use_all=False):
        '''
        Adiciona arestas de sobreposição forçada
        '''
        list_isolados = list(nx.isolates(self))

        # Selecionar setores ainda não vinculados
        list_isolados_A = [i.split('.')[-1] for i in list_isolados if i.startswith('A.')]
        isolados_A = self.m1.query('ID in @list_isolados_A')
        isolados_A = isolados_A.rename(columns={k:f'A.{k}' for k in isolados_A.columns})
        isolados_A['A.geometry'] = isolados_A['A.geometry'].buffer(buffer)
        isolados_A = isolados_A.set_geometry('A.geometry', crs=self.m1.crs)
        if len(isolados_A) > 0:
            if not use_all:
                buffered_B = self.m2.query('CLASS != "Manutenção"')
            else:
                buffered_B = self.m2.copy()
            buffered_B['B.geometry'] = buffered_B['geometry'].buffer(buffer)
            buffered_B = buffered_B.set_geometry('B.geometry', crs=self.m2.crs)
            # Interseção
            intersecao = gpd.overlay(isolados_A, buffered_B, how='union')\
                            .dropna(subset=['A.ID', 'ID'])
            intersecao['RESIDUAL'] = intersecao['geometry'].apply(lambda x: geomIsResidual(x, self.ap_ratio))
            for _, row in intersecao.query('RESIDUAL == False').iterrows():
                self.add_edge(f"A.{row['A.ID']}",
                                f"B.{row['ID']}",
                                metodo=f'Sobreposição ({buffer}m)')

        # Selecionar setores ainda não vinculados
        list_isolados_B = [i.split('.')[-1] for i in list_isolados if i.startswith('B.')]
        isolados_B = self.m2.query('ID in @list_isolados_B')
        isolados_B = isolados_B.rename(columns={k:f'B.{k}' for k in isolados_B.columns})
        isolados_B['B.geometry'] = isolados_B['B.geometry'].buffer(buffer)
        isolados_B = isolados_B.set_geometry('B.geometry', crs=self.m2.crs)
        if len(isolados_B) > 0:
            if not use_all:
                buffered_A = self.m1.query('CLASS != "Manutenção"')
            else:
                buffered_A = self.m1.copy()
            buffered_A['A.geometry'] = buffered_A['geometry'].buffer(buffer)
            buffered_A = buffered_A.set_geometry('A.geometry', crs=self.m1.crs)
            # Interseção
            intersecao = gpd.overlay(isolados_B, buffered_A, how='union')\
                            .dropna(subset=['ID', 'B.ID'])
            intersecao['RESIDUAL'] = intersecao['geometry'].apply(lambda x: geomIsResidual(x, self.ap_ratio))
            for _, row in intersecao.query('RESIDUAL == False').iterrows():
                self.add_edge(f"A.{row['ID']}",
                                f"B.{row['B.ID']}",
                                metodo=f'Sobreposição ({buffer}m)')
            
        self.buffer.append({'buffer':buffer, 'use_all':use_all})
        self.clearGroups()

    def clearGroups(self):
        '''
        Remove arestas que não pertencem ao mesmo grupo
        '''
        to_remove = []
        for u, v, d in self.edges(data=True):
            group_A = self.nodes[u]['group']
            group_B = self.nodes[v]['group']
            if group_A != group_B:
                to_remove.append([u, v])

        for u, v in to_remove:
            # Remove as arestas entre grupos apenas de nós que tem vínculos válidos
            if not all([e in to_remove for e in self.edges(u)]) and not all([e in to_remove for e in self.edges(v)]):
                self.remove_edge(u,v)

    @timer
    def assessEdges(self, ground_truth_graph):
        '''
        Avalia grafo gerado com um grafo contendo as verdadeiras correspondências
        '''
        # Operações entre grafos
        symdiff = nx.symmetric_difference(ground_truth_graph, self)
        false_neg = nx.difference(ground_truth_graph, self)
        true_pos = nx.intersection(ground_truth_graph, self)
        false_pos = nx.difference(symdiff, ground_truth_graph)

        # Cria df de avaliação
        self.assess_df = pd.DataFrame([{'u':i, 'v':j} for i, j in list(true_pos.edges())])
        self.assess_df['assessment'] = 'true positive'

        df_fp = pd.DataFrame([{'u':i, 'v':j} for i, j in list(false_pos.edges())])
        self.assess_df = pd.concat([self.assess_df, df_fp])
        self.assess_df['assessment'] = self.assess_df['assessment'].fillna('false positive')

        df_fn = pd.DataFrame([{'u':i, 'v':j} for i, j in list(false_neg.edges())])
        self.assess_df = pd.concat([self.assess_df, df_fn])
        self.assess_df['assessment'] = self.assess_df['assessment'].fillna('false negative')

        # Cálculo pontuação
        recall = len(true_pos.edges())/(len(true_pos.edges())+len(false_neg.edges()))
        precision = len(true_pos.edges())/len(self.edges)
        f1_score = 2/((recall**-1) + (precision**-1))
        self.scores = {
            'true positives':len(true_pos.edges()),
            'false positives':len(false_pos.edges()),
            'false negatives':len(false_neg.edges()),
            'recall':recall,
            'precision':precision,
            'f1_score':f1_score
        }
        self.assessed = True


    def _codifyCompat(self):
        '''
        Codifica as componentes do grafo resultante
        '''
        componentes = [{'group':self.nodes[list(G)[0]]['group'], 'nodes':list(G)}\
               for G in nx.connected_components(self)]
        increment_id = {k['group']:0 for k in componentes}

        matriz_A = []
        matriz_B = []
        for c in componentes:
            increment_id[c['group']] += 1
            cod_c = f"{c['group']}{increment_id[c['group']]:05d}"

            c_A = [self.nodes[i]['nome'] for i in c['nodes'] if self.nodes[i]['malha']=='A']
            matriz_A.append({'CD_MCA':cod_c, 'ID':c_A})
            c_B = [self.nodes[i]['nome'] for i in c['nodes'] if self.nodes[i]['malha']=='B']
            matriz_B.append({'CD_MCA':cod_c, 'ID':c_B})

        # Criação dos DataFrames finais
        df_matriz_A = pd.DataFrame(matriz_A)
        self.compatTable_A = df_matriz_A.explode('ID')
        df_matriz_B = pd.DataFrame(matriz_B)
        self.compatTable_B = df_matriz_B.explode('ID')


    def _prepareExport(self, mca_base='B'):
        '''
        Cria tabelas de exportação
        '''
        self._codifyCompat()
        # Contagem de membros A dos perímetros
        data_matriz_A = self.compatTable_A.pivot_table(index='CD_MCA',
                                                values='ID',
                                                aggfunc='count').reset_index()
        data_matriz_A = data_matriz_A.rename(columns={'ID':'membros_A'})
        # Contagem de membros B dos perímetros
        data_matriz_B = self.compatTable_B.pivot_table(index='CD_MCA',
                                                values='ID',
                                                aggfunc='count').reset_index()
        data_matriz_B = data_matriz_B.rename(columns={'ID':'membros_B'})
        # Agregação dos dados
        data_matrizes = data_matriz_A.merge(data_matriz_B, on='CD_MCA')
        data_matrizes['membros'] = data_matrizes['membros_A'] + data_matrizes['membros_B']
        if mca_base=='A':
            gdf_pc = self.compatTable_A.merge(self.m1, on='ID')
        else:
            gdf_pc = self.compatTable_B.merge(self.m2, on='ID')
        gdf_pc = gdf_pc.merge(data_matrizes, on='CD_MCA')
        gdf_pc = gpd.GeoDataFrame(gdf_pc, 
                                  geometry='geometry', 
                                  crs=self.m1.crs if mca_base=='A' else self.m2.crs)
        gdf_pc = gdf_pc[['CD_MCA', 'GROUP', 'membros', 'membros_A', 'membros_B', 'geometry']].dissolve(by='CD_MCA')
        gdf_pc = gdf_pc.rename(columns={'GROUP':'CD_DIST'})
        gdf_pc['CD_MUN'] = gdf_pc['CD_DIST'].apply(lambda x: x[:7])
        gdf_pc['TIPO_CORRESP'] = gdf_pc.apply(lambda x: format_corresp(x['membros_A'], x['membros_B']), axis=1)
        gdf_pc['geometry'] = gdf_pc['geometry'].apply(removeHoles)
        self.MCA = gdf_pc

    def _prepareGraphExport(self):
        '''
        Cria representação geográfica do grafo de compatibilidade
        '''
        # Arestas
        edge_data = []
        if self.assessed:
            # Adiciona dados de avaliação do grafo e cria arestas provisórias
            for _, r in self.assess_df.iterrows():
                if self.has_edge(r['u'], r['v']):
                    self[r['u']][r['v']].update({'assessment': r['assessment']})
                else:
                    self.add_edge(r['u'], r['v'], metodo='', assessment='false negative')

        # Organiza dados das arestas
        for u, v, edge_dic in list(self.edges(data=True)):
            data_u = self.nodes[u]
            data_u = {f"{data_u['malha']}.{k}":value for k, value in data_u.items()}
            data_v = self.nodes[v]
            data_v = {f"{data_v['malha']}.{k}":value for k, value in data_v.items()}
            data_u.update(data_v)
            data_u.update(edge_dic)
            data_u['geometry'] = LineString([data_u['A.center'], data_u['B.center']])
            edge_data.append(data_u)

        edge_gdf = gpd.GeoDataFrame(edge_data, geometry='geometry', crs=self.m2.crs)

        if self.assessed:
            self.edge_gdf = edge_gdf[['A.nome', 'A.classe', 'B.nome', 'B.classe', 'metodo', 'assessment', 'geometry']]

            # Remover arestas provsórias
            edges_to_remove = []
            for u, v, data in self.edges(data=True):
                if data.get('assessment') == 'false negative': 
                    edges_to_remove.append((u, v))
            self.remove_edges_from(edges_to_remove)
        else:
            self.edge_gdf = edge_gdf[['A.nome', 'A.classe', 'B.nome', 'B.classe', 'metodo', 'geometry']]
       
        # Nós
        node_data = [i for _, i in list(self.nodes.data())]
        for k in node_data:
            k['grau'] = len(self[f"{k['malha']}.{k['nome']}"])
        node_gdf = gpd.GeoDataFrame(node_data, geometry='center', crs=self.m2.crs)
        self.node_gdf = node_gdf[['nome', 'malha', 'classe', 'group', 'grau', 'center']]

    @timer
    def exportCompatFiles(self, compatName, name_C1, name_C2, mca_base='B', dir='results'):
        '''
        Exporta os arquivos de compatibilização
        '''
        self._prepareExport(mca_base=mca_base)
        self._prepareGraphExport()
        self.compatTable_A[['ID', 'CD_MCA']].to_csv(f'{dir}/{compatName}_{name_C1}.csv', sep='\t', index=False)
        self.compatTable_B[['ID', 'CD_MCA']].to_csv(f'{dir}/{compatName}_{name_C2}.csv', sep='\t', index=False)

        self.MCA.to_file(f'{dir}/{compatName}_MCA.gpkg',
                        layer=f'{name_C1}-{name_C2}',
                        driver='GPKG')
        # Exportar representação geográfica do grafo
        self.edge_gdf.to_file(f'{dir}/{compatName}_MCA.gpkg',
                            layer=f'{name_C1}-{name_C2}_edges',
                            driver='GPKG')
        self.node_gdf.to_file(f'{dir}/{compatName}_MCA.gpkg',
                            layer=f'{name_C1}-{name_C2}_nodes',
                            driver='GPKG')

    @timer
    def reportCompat(self, file=None):
        '''
        Calcula e salva resultados e métricas da compatibilização em um arquivo de texto json
        '''
        # Manutenções inconsistentes
        mincs = len(self.node_gdf.query('classe == "Manutenção" & grau > 1'))
        manutencoes = len(self.node_gdf.query('classe == "Manutenção"'))
        if manutencoes:
            pct_mincs = mincs/manutencoes
        else:
            pct_mincs = None


        # Isolados persistentes
        df = (self.edge_gdf.merge(self.compatTable_B, left_on='B.nome', right_on='ID', how='left')
                           .merge(self.MCA.reset_index()[['CD_MCA', 'TIPO_CORRESP']], on='CD_MCA', how='left' ))
        ips = len(df.query('metodo == "Sobreposição (0m)"'))
        pct_ips = ips/len(self.node_gdf)
        
        # Razão de divisões não puras
        try:
            df = df.pivot_table(index='TIPO_CORRESP', columns='metodo', values='B.nome', aggfunc='count')
            dnp = float(df.loc['m:n','Divisão'])
            pct_dnp = float(dnp/df['Divisão'].sum())
        except:
            dnp = None
            pct_dnp = None
        
        # Desconexos
        dcxs = len(self.node_gdf.query('grau == 0'))
        pct_dcxs = dcxs/len(self.node_gdf)

        # Dados de compatibilização
        edge_data = self.edge_gdf.pivot_table(index='metodo', values='geometry', aggfunc='count').to_dict()['geometry']
        tt_edges = len(self.edge_gdf)
        edge_data = {k:{'n':v, 'pct':v/tt_edges} for k, v in edge_data.items()}
        
        # Dados de operações
        mcas_data = self.MCA.pivot_table(index='TIPO_CORRESP', values='geometry', aggfunc='count').to_dict()['geometry']
        tt_mcas = len(self.MCA)
        mcas_data = {k:{'n':v, 'pct':v/tt_mcas} for k, v in mcas_data.items()}

        # Redesenhos extensos
        rdex = len(self.MCA.query('membros_A > 10 & membros_B > 10'))
        pct_rdex = rdex/tt_mcas

        # Estruturação do relatório
        dic = {
            'params':{
                'A/P ratio': self.ap_ratio,
                'Lmin': self.threshold,
                'Buffers': self.buffer
            },
            'layers':{
                'Setores C1': len(self.m1),
                'Setores C2': len(self.m2),
            },
            'relationships':{
                'Total relationships': {'n':tt_edges, 'pct':1},
            },
            'operations':{
                'Total redesign operations': {'n':tt_mcas, 'pct':1},
            },
            'metrics':{
                'Inconsistent maintenance': {'n':mincs, 'pct':pct_mincs},
                'Inconsistent splitting': {'n':dnp, 'pct':pct_dnp},
                'Unconnected polygons': {'n':dcxs, 'pct':pct_dcxs},
                'Persistent unconnected':{'n':ips, 'pct':pct_ips},
                'Large redesigns':{'n':rdex, 'pct':pct_rdex},
            }
        }

        # Atualiza com dados de avaliação
        if self.assessed:
            dic.update({'assessment':self.scores})

        # Adiciona informações de pivot_tables
        dic['relationships'].update(edge_data)
        dic['operations'].update(mcas_data)

        # Salva em arquivo
        if file:
            with open(f'{file}.json', 'w') as fp:
                json.dump(dic, fp)

        return dic
    
def _readTestMetrics(param, dir, agg, assessed):
    '''
    Reads metrics from report files
    '''
    assert param in ['apr', 'lmin']
    assert agg in ['n', 'pct']

    param_name = 'A/P ratio' if param=='apr' else 'Lmin'
    param_files = [i for i in os.listdir(dir) if param in i]
    multiplier = 100 if agg=='pct' else 1

    df = pd.DataFrame()
    
    for file in param_files:
        with open(f'{dir}/{file}' , 'r') as f:
            datalist = [json.load(f)]

        d = [{param_name: x['params'][param_name]} for x in datalist]
        d = [{**i, **j} for i,j in zip(d,[{k:v[agg]*multiplier for k,v in x['metrics'].items()} for x in datalist])]
        df_param = pd.DataFrame(d)

        if param == 'lmin':
            df_param = df_param.explode('Lmin')

        if assessed:
            d = [{param_name: x['params'][param_name]} for x in datalist]
            d = [{**i, **j} for i,j in zip(d,[{k:v for k,v in x['assessment'].items()} for x in datalist])]
            df_assess = pd.DataFrame(d)[[param_name, 'recall', 'precision', 'f1_score']]
            if param == 'lmin':
                df_assess = df_assess.explode('Lmin')
            df_param = df_param.merge(df_assess, on=param_name, how='left')
            for c in ['recall', 'precision', 'f1_score']:
                df_param[c] = df_param[c]*100
        df_param['file']=file
        df_param['agg']=agg
        
        df = pd.concat([df, df_param])

    return df

def readTestReports(param, dir='results/params', assessed=True):
    df1 = _readTestMetrics(param, dir, agg='n', assessed=assessed)
    df2 = _readTestMetrics(param, dir, agg='pct', assessed=assessed)
    return pd.concat([df1, df2])

def readReports(dir='results', assessed=True):
    '''
    Reads metrics from report files
    '''
    param_files = [i for i in os.listdir(dir) if 'report.json' in i]

    outdata = []
    
    for file in param_files:
        with open(f'{dir}/{file}' , 'r') as f:
            data = json.load(f)

        d = data['layers']
        for g in ['relationships', 'operations', 'metrics']:
            d.update({f'{k}_{'n'}':v['n'] for k,v in data[g].items()})
            d.update({f'{k}_{'pct'}':v['pct']*100 for k,v in data[g].items()})
        
        if assessed:
            d.update(data['assessment'])
            for i in ['recall', 'precision', 'f1_score']:
                d[i]=d[i]*100
        d['file']=file
        outdata.append(d)

    return pd.DataFrame(outdata)