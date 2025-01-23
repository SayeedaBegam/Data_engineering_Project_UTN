import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# Dashboard title
st.title('Streamlit Dashboard Example')

# Sidebar for user input
st.sidebar.header('User Settings')
num_points = st.sidebar.slider('Number of Data Points', min_value=10, max_value=1000, value=200)

# Generate random data
data = pd.DataFrame({
    'x': np.arange(num_points),
    'y': np.random.randn(num_points).cumsum()
})

# Display a sample of the data
st.write('Sample Data:', data.head())

# Display the plot
st.subheader('Data Plot')
fig, ax = plt.subplots()
ax.plot(data['x'], data['y'], label='Random Data')
ax.set_xlabel('X-axis')
ax.set_ylabel('Y-axis')
ax.legend()
st.pyplot(fig)

# Conditional display based on user interaction
st.subheader('Data Statistics')
mean_val = data['y'].mean()
std_val = data['y'].std()

st.write(f'Mean Value: {mean_val:.2f}')
st.write(f'Standard Deviation: {std_val:.2f}')
