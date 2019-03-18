# coding: utf-8

"""
Classes for building igraph graphs and visualizing igraphs and Neo4j graphs.
"""

import igraph
import json
from numpy.random import choice as random_choice
import pandas as pd
import pickle
import py2neo
import uuid
from IPython.display import display, HTML, Javascript


class GraphBuilder(object):
    """Class providing methods to create an igraph graph."""

    def __init__(self, vertices, edge_list, source, target, v_ident,
                 directed=False, identifier='identifier'):
        """
        Parameters to build graph.

        --- Required parameters ---
        vertices:   DataFrame or str -- pandas dataframe or path to pickle,
                    Excel or CSV file containing vertices table
        edge_list:  DataFrame or str -- pandas dataframe or path to pickle,
                    Excel or CSV file containing edge list table
        source:     str -- name of variable containing first vertices in
                    edge list table
        target:     str -- name of variable containing second vertices in
                    edge list table
        v_ident:    str -- name of variable holding vertex identifer in
                    vertices table

        --- Optional parameters ---
        directed:   boolean -- defaults to False
        identifier: str -- what the vertex identifier attribute is called in
                    the graph; defaults to 'identifier'
        """
        assert type(source) == str and len(source) > 0, \
            '"source" must be a non-empty string'
        assert type(target) == str and len(target) > 0, \
            '"target" must be a non-empty string'
        assert type(v_ident) == str and len(v_ident) > 0, \
            '"v_ident" must be a non-empty string'
        if type(vertices) != pd.DataFrame:
            ft = vertices.split('.')[-1]
            if ft == 'pkl':
                vertices = pd.read_pickle(vertices)
            elif ft == 'xlsx':
                vertices = pd.read_excel(vertices)
            else:
                vertices = pd.read_csv(vertices)
        if type(edge_list) != pd.DataFrame:
            ft = edge_list.split('.')[-1]
            if ft == 'pkl':
                edge_list = pd.read_pickle(edge_list)
            elif ft == 'xlsx':
                edge_list = pd.read_excel(edge_list)
            else:
                edge_list = pd.read_csv(edge_list)
        self.g = self._make_g(vertices, edge_list, v_ident, source,
                              target, directed, identifier)

    def _make_g(self, vertices, edge_list, v_ident, source, target,
                directed, identifier):
        # create and add vertices
        g = igraph.Graph(len(vertices), directed=directed)
        # add vertex identifier as a vertex attribute and create vertex
        # identifier-to-index mapping
        g.vs[identifier] = vertices[v_ident]
        vtx_id2idx = dict(zip(g.vs[identifier], g.vs.indices))
        # add additional vertex attributes
        v_attrs = sorted(set(vertices.columns).difference([v_ident]))
        for attr in v_attrs:
            g.vs[attr] = vertices[attr]

        # create and add edges and create and add edge ID as an edge attribute
        edges = [tuple([vtx_id2idx[s], vtx_id2idx[t]]) for
                 s, t, in zip(edge_list[source], edge_list[target])]
        g.add_edges(edges)
        g.es['identifier'] = list(zip(edge_list[source], edge_list[target]))
        # add edge attributes
        e_attrs = sorted(set(edge_list.columns).
                         difference([source, target]))
        for attr in e_attrs:
            g.es[attr] = edge_list[attr]
        return g

    def write_graph(self, path=None):
        """
        Write graphs to graphML files and pickle them.

        --- Optional parameter ---
        path: str -- paths to output graphs as graphML and
              pickle files
        """
        if path:
            self.g.write_graphml(path + '.graphml')
            pickle.dump(self.g, open(path + '.pkl', 'wb'))
        return


class Neo4j_iGraph(object):
    """Helper class to convert Neo4j query into an iGraph graph."""

    def __init__(self, query, neo4j_graph, v_ident, e_ident, v_type='type',
                 e_type='type', source='source', target='target'):
        assert type(v_ident) == str and len(v_ident) > 0, \
            '"v_ident" must be a non-empty string'
        assert type(e_ident) == str and len(e_ident) > 0, \
            '"e_ident" must be a non-empty string'
        assert type(v_type) == str and len(v_type) > 0, \
            '"v_type" must be a non-empty string'
        assert type(e_type) == str and len(e_type) > 0, \
            '"e_type" must be a non-empty string'
        assert type(source) == str and len(source) > 0, \
            '"source" must be a non-empty string'
        assert type(target) == str and len(target) > 0, \
            '"target" must be a non-empty string'
        self.g = self._make_g(query, neo4j_graph, v_ident, e_ident, v_type,
                              e_type, source, target)

    def _make_g(self, query, neo4j_graph, v_ident, e_ident, v_type, e_type,
                source, target):
        data = neo4j_graph.run(query).next()
        nodes = list(set(data['sources'] + data['targets']))
        edges = data['edges']
        vertices = []
        for n in nodes:
            vertex = {attr: n.get(attr, '') for attr in n.keys()}
            vertex[v_type] = next(iter(n.labels), '')
            vertex['neo4j_id'] = n.identity
            vertices.append(vertex)
        edge_list = []
        for e in edges:
            edge = {attr: e.get(attr, '') for attr in e.keys()}
            edge[e_type] = next(iter(e.types()), '')
            edge[source] = e.start_node.get(e_ident)
            edge[target] = e.end_node.get(e_ident)
            edge['neo4j_source_id'] = e.start_node.identity
            edge['neo4j_target_id'] = e.end_node.identity
            edge_list.append(edge)
        iG = GraphBuilder(pd.DataFrame(vertices), pd.DataFrame(edge_list),
                          source, target, v_ident, True, v_ident)
        self.write_graph = iG.write_graph
        return iG.g


class GraphVis(object):
    """
    Provides interface to javascript rendering of Neo4J and igraph graphs.

    Provides public method 'vis' for drawing in Jupyter notebooks.

    Modified from https://github/merqurio/neo4jupyter/blob/master/neo4jupyter.py
    """

    def __init__(self, directed=True, height=500, limit=100, physics=True):
        """
        Optional parameters for graph drawing.

        --- Optional parameters ---
        directed: boolean -- Flags whether the graph is directed. Defaults to
                  True.

        height:   int -- Height of the output window in pixels. Defaults
                  to 500.

        limit:    int -- Limit on the number of source vertices to be randomly
                  drawn from a Neo4J graph or the total number of vertices to
                  be drawn from an igraph. Defaults to 100.

        physics:  boolean -- Flags whether the drawing routine should use
                  physics. Defaults to True.

        """
        self.directed = directed
        self.height = height
        self.limit = limit
        self.physics = physics
        self._html_template = """
            <div id='{id}' style='height: {height}px;'></div>

            <script type='text/javascript'>
                var nodes = {nodes};
                var edges = {edges};
                var container = document.getElementById('{id}');
                var data = {{
                    nodes: nodes,
                    edges: edges
                }};
                var options = {{
                nodes: {{
                    shape: 'dot',
                    size: 25,
                    font: {{
                        size: 14
                    }}
                }},
                edges: {{
                    font: {{
                        size: 14,
                        align: 'middle'
                    }},
                    color: 'gray',
                    arrows: {{
                        to: {{
                            enabled: {directed},
                            scaleFactor: 0.5
                        }}
                    }},
                    smooth: {{
                        enabled: false
                    }}
                }},
                physics: {{
                    enabled: {physics}
                    }}
                }};
                var network = new vis.Network(container, data, options);
            </script>
            """
        self._init_notebook_mode()

    def _init_notebook_mode(self):
        """
        Creates a script tag and prints the JS read from the file in the tag.
        """
        display(
            Javascript(data="require.config({ " +
                            "    paths: { " +
                            "        vis: '//cdnjs.cloudflare.com/ajax/libs/" +
                                          "vis/4.8.2/vis.min' " +
                            "    } " +
                            "}); " +
                            "require(['vis'], function(vis) { " +
                            " window.vis = vis; " +
                            "}); ",
                       css='https://cdnjs.cloudflare.com/ajax/libs/vis/4.8.2/' +
                            'vis.css')
        )

    def _vis_graph(self, nodes, edges, directed, height, physics):
        """
        Creates the HTML page.

        --- Required parameters ---
        nodes:   list -- The nodes represented as dicts containing their data.
        edges:   list -- The edges represented as dicts containing their data.

        returns: IPython.display.HTML
        """

        html = self._html_template.format(id=uuid.uuid4(),
                                          height=json.dumps(height),
                                          nodes=json.dumps(nodes),
                                          edges=json.dumps(edges),
                                          directed=json.dumps(directed),
                                          physics=json.dumps(physics))
        return HTML(html)

    # return the dict that represents an edge
    def _get_neo_edge_info(self, rel):
        return({'from': id(rel.start_node), 'to': id(rel.end_node),
                'label': next(iter(rel.types()))})

    # calculate and return the dict that represents a node
    def _get_neo_node_info(self, node, options):
        node_label = next(iter(node.labels), '')
        prop_key = options.get(node_label)
        vis_label = node.get(prop_key, '')
        return {'id': id(node), 'label': vis_label, 'group': node_label,
                'title': repr(node)}

    def _vis_neo_graph(self, graph, options, limit):
        query = ('MATCH (n) WITH n, rand() AS random ORDER BY random' +
                 (' LIMIT ' + str(limit) if limit else '') +
                 ' OPTIONAL MATCH (n)-[r]->(m) RETURN collect(distinct n) AS' +
                 ' sources, collect(distinct m) AS targets,' +
                 ' collect(distinct r) AS edges')

        data = graph.run(query).next()
        _nodes = list(set(data['sources'] + data['targets']))
        _edges = data['edges']
        nodes = [self._get_neo_node_info(n, options) for n in _nodes]
        edges = [self._get_neo_edge_info(r) for r in _edges]

        return nodes, edges

    def _vis_neo_subgraph(self, subgraph, options):
        nodes = [self._get_neo_node_info(n, options) for n in subgraph.nodes]
        edges = [self._get_neo_edge_info(r) for r in subgraph.relationships]
        return nodes, edges

    def _vis_igraph(self, g, options, limit):
        nodes = []
        node_type = options.get('node_type', '')
        vis_labels = options.get('vis_labels', {})
        edge_type = options.get('edge_type', '')
        if limit:
            _g = g.subgraph(random_choice(g.vs.indices, limit, replace=False))
        else:
            _g = g
        for v in _g.vs:
            node_label = v.attributes().get(node_type, '')
            vis_label = v.attributes().get(vis_labels.get(node_label, ''), '')
            nodes.append({'id': v.index, 'label': vis_label,
                          'group': node_label, 'title': repr(v)})
        edges = [{'from': e.source, 'to': e.target,
                  'label': e.attributes().get(edge_type, '')} for e in _g.es]
        return nodes, edges

    def vis(self, g, options={}, directed=None, height=None, limit=None,
            physics=None):
        """
        Public method for garph drawing within notebooks.

        --- Required parameter ---
        g:        py2neo graph or subgraph, or igraph graph

        --- Optional parameters ---
        options:  dict -- For Neo4J graphs or subgraphs, the options argument
                  should be a dictionary of node labels and property keys; it
                  determines which property is displayed for the node label.
                  For example, in the movie graph, options =
                  {'Movie': 'title', 'Person': 'name'}.
                  Omitting a node label from the options dict will leave the
                  node unlabeled in the visualization.

                  For an igraph graph, the options argument should be a
                  dictionary with three keys: 'node_type', 'vis_labels', and
                  'edge_type'. 'node_type' is a string that identifies the
                  vertex attribute that holds which type of node the vertex is.
                  'edge_type' does the same for edges. 'vis_labels' is a
                  dictionary with keys equal to the possible node types and
                  values equal to the attributes by type that should be used to
                  label each node. 'vis_labels' is analogous to the options
                  dictionary passed for Neo4J graphs and subgraphs.

        directed: boolean -- Flags wheter the graph is directed. Defaults to
                  True.

        height:   int -- Height of the output window in pixels. Defaults
                  to 500.

        limit:    int -- Limit on the number of soource vertices to be randomly
                  drawn from a Neo4J graph or total number of vertices to be
                  drawn from an igraph. Defaults to 100.

        physics:  boolean -- Flags wheter the drawing routine should use
                  physics Defaults to True.

        returns: IPython.display.HTML
        """
        directed = directed if directed is not None else self.directed
        height = height if height else self.height
        limit = limit if limit else self.limit
        physics = physics if physics is not None else self.physics
        if type(g) == py2neo.database.Graph:
            nodes, edges = self._vis_neo_graph(g, options, limit)
        elif type(g) == py2neo.data.Subgraph:
            nodes, edges = self._vis_neo_subgraph(g, options)
        elif type(g) == igraph.Graph:
            nodes, edges = self._vis_igraph(g, options, limit)
        else:
            raise TypeError('Graph must be a py2neo graph or subgraph, or an' +
                            ' igraph graph')
        return self._vis_graph(nodes, edges, directed, height, physics)
