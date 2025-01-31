import subprocess
import json
from bs4 import BeautifulSoup
import re

def run_sqlmesh_dag():
    """Run sqlmesh dag command and capture output"""
    try:
        result = subprocess.run(['sqlmesh', 'dag'], capture_output=True, text=True)
        print("Command Output:", result.stdout[:1000])  # Print first 1000 chars of output
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"Error running sqlmesh dag: {e}")
        return None

def extract_graph_data(html_content):
    """Extract nodes and edges from the HTML output"""
    print("HTML Content Length:", len(html_content))
    
    soup = BeautifulSoup(html_content, 'html.parser')
    scripts = soup.find_all('script')
    print(f"Found {len(scripts)} script tags")
    
    # Print all script contents containing 'vis' or 'DataSet'
    for script in scripts:
        if script.string and ('vis' in script.string or 'DataSet' in script.string):
            print("Found relevant script:", script.string[:200])  # First 200 chars
    
    script = soup.find('script', string=re.compile('vis.DataSet'))
    if not script:
        print("No script found with vis.DataSet")
        return None, None
    
    # Extract nodes and edges data using regex
    nodes_match = re.search(r'nodes = new vis\.DataSet\((.*?)\)', script.string, re.DOTALL)
    edges_match = re.search(r'edges: new vis\.DataSet\((.*?)\)', script.string, re.DOTALL)
    
    if not nodes_match:
        print("No nodes match found")
    if not edges_match:
        print("No edges match found")
    
    if not nodes_match or not edges_match:
        return None, None
    
    try:
        nodes = json.loads(nodes_match.group(1))
        edges = json.loads(edges_match.group(1))
        return nodes, edges
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}")
        return None, None

def get_schema_from_id(node_id):
    """Extract schema name from node ID"""
    parts = node_id.split('.')
    if len(parts) >= 2:
        return parts[1].strip('"')
    return None

def convert_to_mermaid(nodes, edges):
    """Convert nodes and edges to Mermaid flowchart format"""
    mermaid_code = ["flowchart LR"]
    
    # Group nodes by schema
    schemas = {}
    for node in nodes:
        schema = get_schema_from_id(node['id'])
        if schema:
            if schema not in schemas:
                schemas[schema] = []
            node_name = node['label'].strip('"')
            schemas[schema].append(node_name)
    
    # Add subgraphs for each schema
    for schema, schema_nodes in schemas.items():
        mermaid_code.append(f"    subgraph {schema}[\"{schema}\"]")
        for node in schema_nodes:
            node_id = node.replace('.', '_').replace('-', '_')
            mermaid_code.append(f"        {node_id}([\"{node}\"])")
        mermaid_code.append("    end")
        mermaid_code.append("")
    
    # Add relationships
    for edge in edges:
        from_node = edge['from'].split('.')[-1].strip('"').replace('.', '_').replace('-', '_')
        to_node = edge['to'].split('.')[-1].strip('"').replace('.', '_').replace('-', '_')
        mermaid_code.append(f"    {from_node} --> {to_node}")
    
    return "\n".join(mermaid_code)

def main():
    # Run sqlmesh dag and get output
    html_output = run_sqlmesh_dag()
    
    print(html_output)
    if not html_output:
        return
    
    # Extract nodes and edges
    nodes, edges = extract_graph_data(html_output)
    if not nodes or not edges:
        print("Failed to extract graph data")
        return
    
    # Convert to Mermaid
    mermaid_code = convert_to_mermaid(nodes, edges)
    
    # Print the Mermaid code
    print(mermaid_code)
    
    # Optionally save to file
    with open('dag_diagram.mmd', 'w') as f:
        f.write(mermaid_code)

if __name__ == "__main__":
    main()