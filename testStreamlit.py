import numpy as np
import streamlit as st
import plotly.graph_objects as go

# Load the data from .npy file
data_dict = np.load(r'C:/Users/RDCRLWKR/Documents/FileCloud/My Files/Active Projects/Snow Working Group/Pivox/Technical/Data/elevation_data_all.npy', allow_pickle=True).item()

filtered_dict = {key: values for key, values in data_dict.items() if key.endswith('_2000') or key.endswith('_2001')}

fig = go.Figure()
# Loop through the dictionary and add a trace for each entry
for category, values in filtered_dict.items():
    fig.add_trace(go.Histogram(
        x=values,
        name=category,
        marker=dict(
            color='lightblue',      # Set the bar color to light blue
            line=dict(
                color='black',      # Set the outline color to black
                width=1             # Set the outline width
            )
        ),
        nbinsx=100,
        visible=False     # Set opacity for better visibility
    ))

fig.data[0].visible = True

# Create buttons for each trace
buttons = []
for i, category in enumerate(filtered_dict.keys()):
    buttons.append(dict(
        label=category,
        method='update',
        args=[{'visible': [j == i for j in range(len(filtered_dict))]},  # Toggle visibility
               {'title': f'Snow Depth Histogram for {category}'}]  # Update title
    ))

# Add buttons to the layout
fig.update_layout(
    title=f'Snow Depth Histogram for: {list(filtered_dict.keys())[0]}',
    xaxis_title='Snow Depth (m)',
    yaxis_title='Count',
    barmode='overlay',
    updatemenus=[{
        'buttons': buttons,
        'direction': 'down',
        'showactive': True,
        'x': 0.0,
        'xanchor': 'left',
        'y': 1.0,
        'yanchor': 'top'
    }]
)

# Show the figure
fig.show()