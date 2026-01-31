#!/usr/bin/env python
# coding: utf-8

# In[1]:


# Importing important libraries 
import pandas as pd
import json
import sqlite3



# In[4]:


# Step 1: Load CSV Data

orders_df = pd.read_csv('orders.csv')
orders_df.head()


# In[5]:


# Step 2: Load JSON Data

users_df = pd.read_json('users.json')
users_df.head()




# In[6]:


# Step 3: Load SQL Data
# We create a temporary in-memory connection to read the .sql file
conn = sqlite3.connect(':memory:')
with open('restaurants.sql', 'r') as f:
    conn.executescript(f.read())


# In[7]:


# Assuming the table in the SQL file is named 'restaurants'
restaurants_df = pd.read_sql_query("SELECT * FROM restaurants", conn)
conn.close()


# In[9]:


restaurants_df.head()


# In[ ]:





# In[ ]:


# Query the table (assuming the table inside is named 'restaurants')
restaurants_df = pd.read_sql_query("SELECT * FROM restaurants", conn)
conn.close()

# Step 4: Merge the Data
# Perform a Left Join: Orders is the primary table
# 1. Join Orders with Users
merged_df = pd.merge(orders_df, users_df, on='user_id', how='left')

# 2. Join the result with Restaurants
final_df = pd.merge(merged_df, restaurants_df, on='restaurant_id', how='left')

# Display the result
print(final_df.head())


# In[10]:


# --- Step 4: Merge the Data ---
# First merge: Orders and Users
merged_df = pd.merge(orders_df, users_df, on='user_id', how='left')


# In[13]:


# Second merge: Result + Restaurants
final_df = pd.merge(merged_df, restaurants_df, on='restaurant_id', how='left')


# In[20]:


# --- Step 5: Create Final Dataset ---
final_df.to_csv('final_food_delivery_dataset.csv', index=False)
df=pd.read_csv("final_food_delivery_dataset.csv")

df.head()


# In[60]:


df.info()


# In[61]:


df.shape


#    ## 1 Which city has the highest total revenue (total_amount) from Gold members?  

# In[21]:


# Filter for Gold members only
gold_members = df[df['membership'] == 'Gold']

# Group by city and sum the total_amount
city_revenue = gold_members.groupby('city')['total_amount'].sum().reset_index()



# In[23]:


# Find the city with the maximum revenue
# We sort descending and take the first row
highest_revenue_city = city_revenue.sort_values(by='total_amount', ascending=False).iloc[0]

print(f"The city with the highest total revenue from Gold members is: {highest_revenue_city['city']}")
print(f"Total Revenue: {highest_revenue_city['total_amount']:.2f}")


# ## 2 Which cuisine has the highest average order value across all orders?

# In[25]:


# Calculate the average total_amount for each cuisine
cuisine_avg_value = df.groupby('cuisine')['total_amount'].mean().reset_index()

# Sort to find the highest value
highest_avg = cuisine_avg_value.sort_values(by='total_amount', ascending=False).iloc[0]

print(f"The cuisine with the highest average order value is: {highest_avg['cuisine']}")
print(f"Average Order Value: {highest_avg['total_amount']:.2f}")


# ## 3 How many distinct users placed orders worth more than 1000 in total (sum of all their orders)?

# In[26]:


# 1. Group by user_id and sum their total_amount
user_spending = df.groupby('user_id')['total_amount'].sum()

# 2. Filter for users whose total sum is greater than 1000
high_spenders = user_spending[user_spending > 1000]

# 3. Get the count of these distinct users
distinct_count = len(high_spenders)

print(f"Number of distinct users with total orders > 1000: {distinct_count}")


# ## 4 Which restaurant rating range generated the highest total revenue?

# In[30]:


# 1. Define the ranges (bins) and their labels
bins = [3.0, 3.5, 4.0, 4.5, 5.0]
labels = ['3.0-3.5', '3.6-4.0', '4.1-4.5', '4.6-5.0']

# 2. Assign each restaurant to a rating range
df['rating_range'] = pd.cut(df['rating'], bins=bins, labels=labels, include_lowest=True)

# 3. Sum the revenue for each range
range_revenue = df.groupby('rating_range')['total_amount'].sum().reset_index()

# 4. Identify the top performing range
top_range = range_revenue.sort_values(by='total_amount', ascending=False).iloc[0]

print(f"The rating range with the highest revenue is {top_range['rating_range']} stars.")
print(f"Total Revenue: {top_range['total_amount']:.2f}")


# ## 5 Among Gold members, which city has the highest average order value?

# In[32]:


# Filter the data to include only 'Gold' members
gold_members = df[df['membership'] == 'Gold']

# Group by city and calculate the mean (average) of total_amount
city_aov = gold_members.groupby('city')['total_amount'].mean().reset_index()

# Identify the city with the maximum average order value
highest_aov = city_aov.sort_values(by='total_amount', ascending=False).iloc[0]

print(f"The city with the highest average order value for Gold members is: {highest_aov['city']}")
print(f"Average Order Value: {highest_aov['total_amount']:.2f}")


# ## 6 Which cuisine has the lowest number of distinct restaurants but still contributes significant revenue?

# In[33]:


# Aggregate: Count unique restaurants and sum revenue for each cuisine
cuisine_analysis = df.groupby('cuisine').agg(
    unique_restaurant_count=('restaurant_id', 'nunique'),
    total_revenue=('total_amount', 'sum')
).reset_index()

# Calculate Revenue per Restaurant (efficiency metric)
cuisine_analysis['revenue_per_restaurant'] = (
    cuisine_analysis['total_revenue'] / cuisine_analysis['unique_restaurant_count'])

# Sort by fewest restaurants and highest revenue
result = cuisine_analysis.sort_values(
    by=['unique_restaurant_count', 'total_revenue'], 
    ascending=[True, False])

print("Cuisine Performance Analysis:")
print(result)

top_efficient_cuisine = result.iloc[0]['cuisine']
print(f"\nThe cuisine with the fewest restaurants but highest impact is: {top_efficient_cuisine}")


# ## 7 What percentage of total orders were placed by Gold members? (Rounded to nearest integer)

# In[34]:


# Count the total number of orders
total_orders = len(df)

# Count the number of orders placed by Gold members
gold_orders = len(df[df['membership'] == 'Gold'])

# Calculate the percentage and round to the nearest integer
percentage_gold = round((gold_orders / total_orders) * 100)

print(f"Total Orders: {total_orders}")
print(f"Orders by Gold Members: {gold_orders}")
print(f"Percentage: {percentage_gold}%")


# ## 8 Which restaurant has the highest average order value but less than 20 total orders?

# In[41]:


# Calculate average order value and total order count per restaurant
# We use 'restaurant_name_y' which comes from the original restaurants dataset
restaurant_metrics = df.groupby('restaurant_name_x').agg(
    avg_order_value=('total_amount', 'mean'),
    order_count=('order_id', 'count')
).reset_index()

# Filter for restaurants with less than 20 orders
low_volume_restaurants = restaurant_metrics[restaurant_metrics['order_count'] < 20]
low_volume_restaurants

# 4. Find the restaurant with the highest average order value within this group
result = low_volume_restaurants.sort_values(by='avg_order_value', ascending=False).iloc[0]

print(f"The restaurant with the highest AOV (under 20 orders) is: {result['restaurant_name_x']}")
print(f"Average Order Value: {result['avg_order_value']:.2f}")
print(f"Total Orders: {result['order_count']}")


# ## 9 Which combination contributes the highest revenue?

# In[47]:


# Group by both City and Cuisine and sum the total_amount
combo_revenue = df.groupby(['membership', 'cuisine'])['total_amount'].sum().reset_index()

# Sort to find the highest revenue combination
top_combination = combo_revenue.sort_values(by='total_amount', ascending=False).iloc[0]

print(f"The highest revenue combination is: {top_combination['membership']} +({top_combination['cuisine']})")
print(f"Total Revenue: {top_combination['total_amount']:.2f}")


# ## 10 During which quarter of the year is the total revenue highest?

# In[50]:


# 2. Convert order_date column to datetime objects
df['order_date'] = pd.to_datetime(df['order_date'])

# 3. Create a 'quarter' column (e.g., '2023Q4')
df['quarter'] = df['order_date'].dt.to_period('Q').astype(str)

# 4. Group by quarter and sum the total_amount
quarterly_revenue = df.groupby('quarter')['total_amount'].sum().reset_index()

# 5. Sort to find the quarter with the highest revenue
highest_quarter = quarterly_revenue.sort_values(by='total_amount', ascending=False).iloc[0]

print("Revenue by Quarter:")
print(quarterly_revenue)

print(f"\nThe quarter with the highest total revenue is: {highest_quarter['quarter']}")
print(f"Total Revenue: {highest_quarter['total_amount']:.2f}")


# ## 11 How many total orders were placed by users with Gold membership?

# In[52]:


# Filter for rows where membership is 'Gold' and count them
gold_orders_count = len(df[df['membership'] == 'Gold'])

print(f"Total orders placed by Gold members: {gold_orders_count}")


# ## 12 What is the total revenue (rounded to nearest integer) generated from orders placed in Hyderabad city?

# In[53]:


# Sum the total_amount for rows where the city is 'Hyderabad'
hyderabad_total = df[df['city'] == 'Hyderabad']['total_amount'].sum()

# Round to the nearest integer
final_revenue = round(hyderabad_total)

print(f"Total revenue from Hyderabad: {final_revenue}")


# ## 13 How many distinct users placed at least one order?

# In[54]:


# Count the number of unique entries in the user_id column
distinct_users = df['user_id'].nunique()

print(f"Total number of distinct users: {distinct_users}")


# ## 14 What is the average order value (rounded to 2 decimals) for Gold members?

# In[57]:


# Filter for Gold members and calculate the mean
gold_aov = df[df['membership'] == 'Gold']['total_amount'].mean()

# Print the result rounded to 2 decimal places
print(f"The average order value for Gold members is: {gold_aov:.2f}")


# ## How many orders were placed for restaurants with rating ≥ 4.5?

# In[58]:


# Filter the dataframe for ratings >= 4.5
# Use len() to count the number of matching rows
high_rated_orders_count = len(df[df['rating'] >= 4.5])

print(f"Number of orders for restaurants with rating ≥ 4.5: {high_rated_orders_count}")


# ## How many orders were placed in the top revenue city among Gold members only?

# In[59]:


# Filter data for Gold members only
gold_members = df[df['membership'] == 'Gold']

# Identify the city with the highest total revenue for Gold members
city_revenue = gold_members.groupby('city')['total_amount'].sum()
top_revenue_city = city_revenue.idxmax()

# Count the orders placed in that specific city by Gold members
gold_order_count = len(gold_members[gold_members['city'] == top_revenue_city])

print(f"The top revenue city for Gold members is: {top_revenue_city}")
print(f"Total orders placed by Gold members in that city: {gold_order_count}")


# In[ ]:




