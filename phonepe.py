import numpy as np
import streamlit as st
from streamlit_option_menu import option_menu
import pymysql
import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px 
import seaborn as sns
import warnings

warnings.filterwarnings('ignore')

# Streamlit configuration
st.set_page_config(layout="wide")
st.title("PhonePe Data Visualization and Exploration")

# Sidebar Menu using selectbox
menu_option = st.sidebar.selectbox('Main Menu', ['Home', 'Data Visualization'])

# Database connection (MySQL)
def get_connection():
    mydb_conn = pymysql.connect(
     host = 'localhost',
     user = 'root',
     password = 'root',
     database = 'phonepe_transactions'
    )
    return mydb_conn

# Query functions
def get_decoding_transaction_dynamics(query_type):
    conn = get_connection()
    
    if query_type == 'Regional Performance Analysis':
        query = """
                    WITH yearlyRegionalPerformance AS (
            SELECT
                States,
                years,
                SUM(Transaction_amount) AS total_transaction_amount
            FROM 
                aggregated_transaction
            GROUP BY 
                States, years
        ),

        RankedRegions AS (
            SELECT 
                States, 
                years, 
                total_transaction_amount,
                RANK() OVER (PARTITION BY years ORDER BY total_transaction_amount ASC) AS `rank`
            FROM 
                yearlyRegionalPerformance
        )

        SELECT 
            States, 
            years, 
            total_transaction_amount,
            `rank`
        FROM 
            RankedRegions
        WHERE 
            `rank` <= 5
        ORDER BY 
            years, `rank`;
        """
    elif query_type == 'Category Insights':
        query = """
            SELECT Transaction_type,
            SUM(Transaction_count) AS total_volume, 
            SUM(Transaction_amount) AS total_revenue,
            SUM(Transaction_amount) / SUM(Transaction_count) AS revenue_per_transaction
            FROM 
                aggregated_transaction
            GROUP BY 
                Transaction_type
            Order by
            total_volume DESC;
        """
    elif query_type == 'Trend Analysis':
        query = """
            SELECT years, Quarter, Transaction_type,
            SUM(Transaction_count) AS total_volume,
			SUM(Transaction_amount) AS total_revenue
            FROM aggregated_transaction
            GROUP BY years, Quarter,Transaction_type
            ORDER BY 
			years, quarter
        """
    elif query_type == 'Investigate Interdependencies':
        query = """
            SELECT 
            States,
            years,
            Quarter,
            Transaction_amount,
            LAG(Transaction_amount) OVER (PARTITION BY States ORDER BY years, Quarter) AS previous_Transaction_amount,
            ((Transaction_amount - LAG(Transaction_amount) OVER (PARTITION BY States ORDER BY years, Quarter)) 
            / LAG(Transaction_amount) OVER (PARTITION BY States ORDER BY years, Quarter)) * 100 AS Growth_percentage
        FROM 
            aggregated_transaction
        ORDER BY 
            States, years, Quarter;
        """
    
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def get_transaction_analysis(query_type):
    conn = get_connection()
    
    if query_type == 'Identifying Top States':
        query = """
          SELECT 
            States,
            SUM(Transaction_amount) AS Total_Transaction_Value
        FROM 
            top_transaction_district
        GROUP BY 
            States
        ORDER BY 
            Total_Transaction_Value DESC
        LIMIT 10;
        """
    elif query_type == 'District Performance Evaluation':
        query = """
           SELECT 
            States,district_name,
            SUM(Transaction_count) AS Total_Transactions,
            SUM(Transaction_amount) AS Total_Transaction_Value
        FROM 
            top_transaction_district
        GROUP BY 
            States,district_name
        ORDER BY 
            Total_Transaction_Value DESC, 
            Total_Transactions DESC
        LIMIT 10;
        """
    elif query_type == 'Pin Code Insights':
        query = """
            SELECT States,
            pincode, 
            SUM(Transaction_count) AS Total_Transactions,
            SUM(Transaction_amount) AS Total_Transaction_Value
        FROM 
            top_transaction_pincode
        GROUP BY 
            pincode,States
        ORDER BY 
            Total_Transactions DESC, 
            Total_Transaction_Value DESC
            LIMIT 10;
        """

    elif query_type == 'Comparative Analysis':
        query = """
                    WITH Total_Transaction AS (
                SELECT SUM(Transaction_amount) AS total_value
                FROM top_transaction_district
            )
            SELECT 
                top_transaction_district.States,
                top_transaction_district.district_name,
                top_transaction_district.years,
                top_transaction_pincode.pincode,       
                COUNT(top_transaction_district.Transaction_count) AS Total_Transactions,
                SUM(top_transaction_district.Transaction_amount) AS Total_Transaction_Value,
                (SUM(top_transaction_district.Transaction_amount) / (SELECT total_value FROM Total_Transaction)) * 100 AS Percentage_Share,
                AVG(top_transaction_district.Transaction_amount) AS Avg_Transaction_Value
            FROM 
                top_transaction_district 
            JOIN
                top_transaction_pincode 
                ON top_transaction_district.States = top_transaction_pincode.States
            GROUP BY 
                top_transaction_district.States,
                top_transaction_district.district_name,
                 top_transaction_district.years,
                top_transaction_pincode.pincode
            ORDER BY 
                Total_Transaction_Value DESC 
                LIMIT 50;
                    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    return df


def get_transaction_market_analysis(query_type):
    conn = get_connection()
    
    if query_type == 'Transaction Volume and Value Analysis':
        query = """
           SELECT States, SUM(Transaction_count) As total_no_transaction,
            SUM(Transaction_amount) As total_value_transaction
            from map_transaction
            GROUP by States

        """
    elif query_type == 'Performance Comparison':
        query = """
            SELECT States,
            SUM(Transaction_count) AS total_transaction_count,
            SUM(Transaction_amount) AS total_transaction_value,
            (SUM(Transaction_count) / (SELECT SUM(Transaction_count) FROM map_transaction)) * 100 AS pct_transaction_count,
            (SUM(Transaction_amount) / (SELECT SUM(Transaction_amount) FROM map_transaction)) * 100 AS pct_transaction_value,

        CASE
                WHEN (SUM(Transaction_count) / (SELECT SUM(Transaction_count) FROM map_transaction)) * 100 > 10 
                    OR (SUM(Transaction_amount) / (SELECT SUM(Transaction_amount) FROM map_transaction)) * 100 > 10 THEN 'Strong Performance'
                WHEN (SUM(Transaction_count) / (SELECT SUM(Transaction_count) FROM map_transaction)) * 100 < 2 
                    AND (SUM(Transaction_amount) / (SELECT SUM(Transaction_amount) FROM map_transaction)) * 100 < 2 THEN 'Underperformance'
                ELSE 'Average Performance'
            END AS performance_category
        FROM 
            map_transaction
        GROUP BY 
            States
        ORDER BY 
            pct_transaction_count DESC, 
            pct_transaction_value DESC;
        """

    elif query_type == 'District-Level Insights':
        query = """
            SELECT States, district_name,SUM(Transaction_amount) as total_revenue, SUM(Transaction_count) as total_count
            from map_transaction
            group by States, district_name
            order by States , total_revenue DESC
            LIMIT 50;

        """
    elif query_type == 'Trends Over Time':
        query = """
            SELECT States, years, Quarter, SUM(Transaction_amount) as total_revenue, AVG(Transaction_amount) as avg_revenue
            FROM 
                map_transaction
            GROUP BY 
                States, years, Quarter
            ORDER BY 
                States, years, Quarter
        """
    
    elif query_type == 'Market Potential and Strategy Development':
        query = """
            SELECT 
                States,
                SUM(Transaction_count) AS total_transactions,
                SUM(Transaction_amount) AS total_revenue,
                SUM(Transaction_amount) / SUM(Transaction_count) AS avg_transaction_value
            FROM 
                map_transaction
            GROUP BY 
                States
            ORDER BY 
                total_transactions DESC, avg_transaction_value ASC;
        """
    
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def get_user_growth_analysis(query_type):
    conn = get_connection()
    
    if query_type == 'User Engagement Analysis':
        query = """
          SELECT States, district_name, SUM(registered_user) as total_registered_users,  
          COUNT(distinct registered_user) as total_users,avg(appOpens) as avg_appopens
             from map_user
            group by States, district_name

        """
    elif query_type == 'Performance Comparison':
        query = """
            SELECT map_user.States,
            district_name,
            SUM(registered_user) as total_registered_user,
            SUM(appOpens) As total_appopens,
            SUM(aggregated_user.User_count) As total_active_users,
            AVG(aggregated_user.user_percentage) AS avg_user_percentage
            From map_user
            JOIN aggregated_user
            ON aggregated_user.States = map_user.States
            group by map_user.States, map_user.district_name
            order by total_active_users DESC

        """

    elif query_type == 'Trend Analysis Over Time':
        query = """
            SELECT States, district_name, years, Quarter,
            SUM(registered_user) as total_user,
            SUM(appOpens) as total_appopens
            from map_user
            GROUP BY 
            States, district_name, years, Quarter
            ORDER BY 
            years ASC, Quarter ASC, States, district_name;
        """

    elif query_type == 'Identifying High-Value Markets':
        query = """
           SELECT 
            States,
            district_name,
            SUM(registered_user) AS total_registered_user,
            SUM(appOpens) AS total_appopens,
            CASE 
                WHEN SUM(registered_user) = 0 THEN 0
                ELSE CAST(SUM(appOpens) AS FLOAT) / SUM(registered_user)
            END AS app_open_ratio
        FROM map_user
        GROUP BY States, district_name
        ORDER BY total_registered_user DESC;
        """
    
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def get_user_registration_analysis(query_type):
    conn = get_connection()
    
    if query_type == 'Identifying Top 10 States':
        query = """
          SELECT 
            States, 
            years, 
            quarter, 
            SUM(registeredUsers) AS highest_registered_users
        FROM 
            top_user_district
        GROUP BY 
            States, years, quarter
        ORDER BY 
            highest_registered_users DESC
        LIMIT 10;

        """
    elif query_type == 'Analyze fluctuations in user registration across different quarters and states':
        query = """
          select States,Quarter,SUM(registeredUsers)  AS total_registered_users,
            SUM(registeredUsers) - LAG(SUM(registeredUsers)) OVER (PARTITION BY States ORDER BY quarter) AS change_from_previous_quarter
            from top_user_district 
            GROUP BY
                States, quarter
            ORDER BY
                States, quarter;
        """
    elif query_type == 'District Performance Evaluation':
        query = """
            select States, district_name, SUM(registeredUsers) AS registered_users
            from top_user_district 
            group by States, district_name 
            order by registered_users DESC
            LIMIT 10;
        """

    elif query_type == 'Pin Code Insights':
        query = """
            SELECT States, pincode, SUM(registeredUsers) AS user_registrations
            FROM top_user_pincode
            GROUP BY States, pincode
            ORDER BY user_registrations DESC
            LIMIT 10;

        """

    elif query_type == 'Comparative Analysis':
        query = """   
        select top_user_district.States, district_name, pincode, 
        SUM(top_user_pincode.registeredUsers) As total_registered_users
        from top_user_district
        RIGHT JOIN
        top_user_pincode
        ON top_user_district.States = top_user_pincode.States
        GROUP BY States, district_name, pincode
        ORDER BY total_registered_users DESC;
        """
    
    df = pd.read_sql(query, conn)
    conn.close()
    return df

if menu_option == 'Home':
    st.header("Welcome to PhonePe Data Visualization Dashboard!")
    st.write("Explore various insights and analysis on PhonePe transaction data.")

if menu_option == 'Data Visualization':
    st.header("Data Visualization")
    
    dropdown = st.selectbox('Select one option', [
        "Decoding Transaction Dynamics on PhonePe",
        "Transaction Analysis Across States and Districts",
        "Transaction Analysis for Market Expansion",
        "User Engagement and Growth Strategy",
        "User Registration Analysis"
    ])
    
    if dropdown == "Decoding Transaction Dynamics on PhonePe":
        sub_dropdown = st.selectbox(
            "Select Analysis Type", 
            ["Regional Performance Analysis", "Category Insights","Trend Analysis","Investigate Interdependencies"]
        )

        if sub_dropdown == "Regional Performance Analysis":
            # Fetch data for the selected query
            df = get_decoding_transaction_dynamics(query_type="Regional Performance Analysis")
            
            # Layout for the visualization
            col1, col2 = st.columns(2)
            with col1:
                plt.figure(figsize=(10, 6))
                sns.barplot(
                    x='years', 
                    y='total_transaction_amount', 
                    data=df, 
                    palette='coolwarm',
                    hue= 'States',
                    width=1.5 
                )
                plt.title('Regional Performance Analysis')
                plt.xlabel('years')
                plt.ylabel('Transaction Amount')
                plt.xticks(rotation=45)
                st.pyplot(plt)

        elif sub_dropdown == "Category Insights":
            # Fetch data for the selected query
            df = get_decoding_transaction_dynamics(query_type="Category Insights")
            
            # Layout for the visualization
            col1, col2 = st.columns(2)
            with col1:
                plt.figure(figsize=(10, 6))
                sns.barplot(
                    x='Transaction_type', 
                    y='total_revenue', 
                    data=df, 
                    color='lightcoral', 
                    label='Transaction Volume'
                )
                plt.title('Distribution of Total Transaction Amount')
                plt.xlabel('Transaction Category')
                plt.ylabel('Amount')
                plt.xticks(rotation=45)
                st.pyplot(plt)

            # Second Visualization: Pie Chart for Transaction Categories
            with col2:
               transaction_data = df.groupby('Transaction_type')['total_volume'].sum()
               
               plt.figure(figsize=(8, 8))
               plt.pie(
                    transaction_data, 
                    labels=transaction_data.index, 
                    autopct='%.2f%%', 
                    colors=sns.color_palette("RdBu", len(transaction_data)), 
                    startangle=90
                )
               plt.title("Distribution of Total Transaction Count")
               st.pyplot(plt)

        elif sub_dropdown == "Trend Analysis":
            # Fetch data for the selected query
            df = get_decoding_transaction_dynamics(query_type="Trend Analysis")
            # Create a dropdown for selecting the year
            years = df['years'].unique() 
            selected_year = st.selectbox("Select Year", options=sorted(years))
            # Filter the data based on the selected year
            filtered_df = df[df['years'] == selected_year]
            col1, col2 = st.columns(2)
            with col1:
                plt.figure(figsize=(10,6))
                sns.barplot(data=filtered_df, x='Quarter', y='total_revenue', hue='Transaction_type', palette='Set1')
                plt.title(f'Transaction Amount Distribution -{selected_year}',fontsize=14)
                plt.xlabel('Quarter',fontsize=12)
                plt.ylabel('Revenue',fontsize=12)
                plt.xticks(rotation=45)
                st.pyplot(plt)

        elif sub_dropdown == "Investigate Interdependencies":
            # Fetch data for the selected query
            df = get_decoding_transaction_dynamics(query_type="Investigate Interdependencies")
            col1,col2 = st.columns(2)
            with col1:
               # Create the figure for the plot
                plt.figure(figsize=(12, 6))

                # Plot: Growth Percentage Over Years (for each state)
                sns.lineplot(data=df, x='years', y='Growth_percentage')

                # Title and labels
                plt.title('Transaction Amount Growth Percentage Over Years')
                plt.xlabel('Year')
                plt.ylabel('Growth Percentage (%)')
                plt.xticks(rotation=45)
                plt.tight_layout()
                plt.grid()
                st.pyplot(plt)

    elif dropdown == "Transaction Analysis Across States and Districts":
        sub_dropdown = st.selectbox(
            "Select Analysis Type", 
            ["Identifying Top States","District Performance Evaluation","Pin Code Insights","Comparative Analysis","Strategic Recommendations for Engagement"]
        )    

        if sub_dropdown == "Identifying Top States":
            # Fetch data for the selected analysis type
            df = get_transaction_analysis(query_type="Identifying Top States")
            col1,col2 = st.columns(2)
            with col1:
                plt.figure(figsize=(5, 3))
                sns.barplot(x='States', y='Total_Transaction_Value', data=df, palette='viridis')
                plt.title(f"Top 10 States by Total Transactions")
                plt.xlabel('States')
                plt.ylabel('Total Transactions Amount')
                plt.xticks(rotation=60, ha='right')
                st.pyplot(plt)
        
        elif sub_dropdown == "District Performance Evaluation":
                df = get_transaction_analysis(query_type="District Performance Evaluation")
                col1,col2 = st.columns(2)
                with col1:
                    # Create the first barplot for Transaction Count
                    sns.barplot(x='Total_Transactions', y='district_name', data=df, palette='Blues_d')
                    plt.title('Top 10 Districts by Total Transaction Count')
                    plt.tight_layout()
                    st.pyplot(plt)
                with col2:
                    # Create the second barplot for Transaction Value
                    sns.barplot(x='Total_Transaction_Value', y='district_name', data=df, palette='Oranges_d')
                    plt.title('Top 10 Districts by Total Transaction Value')
                    plt.tight_layout()
                    st.pyplot(plt)

        elif sub_dropdown == "Pin Code Insights":
                df = get_transaction_analysis(query_type="Pin Code Insights")
                col1,col2 = st.columns(2)
                with col1:
                    plt.figure(figsize=(5,7))
                    sns.barplot(x ='pincode', y = 'Total_Transactions', palette='Set2', hue='States', data= df)
                    plt.xlabel('Pincode')
                    plt.ylabel('Total Transactions')
                    plt.title('Top 10 pincodes most Transactions')
                    plt.xticks(rotation = 60, ha='right')
                    plt.tight_layout()
                    st.pyplot(plt)
                with col2:
                    plt.figure(figsize=(5,7))
                    sns.barplot(x ='pincode', y ='Total_Transaction_Value', hue='States', palette='tab10', data=df)
                    plt.xlabel('Pincode')
                    plt.ylabel('Total Transactions amount')
                    plt.title('Top 10 pincodes most Transactions amount')
                    plt.xticks(rotation = 60, ha='right')
                    plt.tight_layout()
                    st.pyplot(plt)
        elif sub_dropdown == "Comparative Analysis":
             df = get_transaction_analysis(query_type="Comparative Analysis")
             col1,col2 = st.columns(2)
             with col1:
                 fig = px.bar(df, 
                    x='district_name', 
                    y='Total_Transaction_Value', 
                    color='Percentage_Share', 
                    facet_col='years',
                    title="Top 50 Districts by Transaction Value and percentage share",
                    labels={'district_name': 'District', 'Total_Transaction_Value': 'Transaction Value'},
                    color_continuous_scale='Viridis')

                # Show the plot
                 fig.update_layout(xaxis_title='District', 
                                yaxis_title='Transaction Value', 
                                xaxis_tickangle=-45)
                 st.plotly_chart(fig)
                 
             with col2:
                plt.figure(figsize=(14, 6))
                sns.scatterplot(x='Total_Transactions', y='Total_Transaction_Value', hue='district_name', data=df)
                plt.title('Comparing Transaction Value vs. Count Across Regions')
                plt.xlabel('Transaction Count')
                plt.ylabel('Transaction Value')
                plt.xticks(rotation=45)
                plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
                plt.tight_layout()
                st.pyplot(plt)
        
        elif sub_dropdown == "Strategic Recommendations for Engagement":
            st.write("""
                        ### Recommendations for Targeted Marketing Strategies:

                        1. High-Performing Areas Offer more promotions and rewards to further increase user engagement.
                        2. Underperforming Areas Create special deals or discounts to attract more users.
                        3. Customize products to match the preferences of users in different regions.
                        4. Run marketing campaigns based on regional performance to boost engagement.
                        """)
            
            st.write("""
                         ### Initiatives to encourage transaction growth in regions with lower activity
                        1. Offer discounts, cashback, or rewards to encourage transactions in low-activity areas.
                        2. Use local ads to raise awareness of your products.
                        3. Partner with local businesses for exclusive deals.
                        4. Introduce a loyalty program to reward repeat customers.
                        5. Simplify the transaction process to make it easier for users.
                        6. Provide incentives like discounts for first-time users to boost activity.
                        """)

    elif dropdown == "Transaction Analysis for Market Expansion":
        sub_dropdown = st.selectbox(
            "Select Analysis Type", 
            ["Transaction Volume and Value Analysis", "Performance Comparison","District-Level Insights","Trends Over Time","Market Potential and Strategy Development"]
        )
        
        if sub_dropdown == "Transaction Volume and Value Analysis":
            # Fetch data for the selected analysis type
            df = get_transaction_market_analysis(query_type="Transaction Volume and Value Analysis")
            fig = px.bar(df, 
                        x='States', 
                        y='total_value_transaction', 
                        color='States',
                        title="Comparison of Transaction Value by State",
                        labels={'States': 'State', 'total_value_transaction': 'Total Value Transactions'},
                        color_continuous_scale='magma')
            fig.update_layout(
                xaxis_title='States',
                yaxis_title='Total Value Transactions',
                xaxis_tickangle=-60
            )
            st.plotly_chart(fig)

        elif sub_dropdown == "Performance Comparison":

            df = get_transaction_market_analysis(query_type="Performance Comparison")
            col1, col2 = st.columns(2)
            with col1:
             fig = px.pie(df, 
                names='States', 
                values='total_transaction_value', 
                title='Proportion of Transaction Value by State',
                color='States',  # Optional: Add color differentiation for each state
                color_discrete_sequence=px.colors.qualitative.Set3,
                hole=0.4)
             st.plotly_chart(fig)
              
            with col2:
             fig2 = px.bar(df, 
             x='States', 
             y='total_transaction_value', 
             color='performance_category', 
             title='State-wise Transaction Value by Performance Category',
             labels={'States': 'State', 'total_transaction_value': 'Total Transaction Value','performance_category':'Performance Category'},
             color_discrete_sequence=px.colors.qualitative.Set3,
             barmode='stack')

            fig2.update_layout(
                xaxis_title='States',
                yaxis_title='Total Transaction Value',
                xaxis_tickangle=90,
                title_font_size=16
            )
            st.plotly_chart(fig2)

        elif sub_dropdown == "District-Level Insights":
            # Fetch data for the selected analysis type
            df = get_transaction_market_analysis(query_type="District-Level Insights")
            fig = px.bar(df, 
             x='district_name', 
             y= 'total_revenue', 
             title='Transaction Growth and Success by top 50 District-level',
             labels={'district_name': 'District', 'total_count': 'Total Transaction Count', 'total_revenue': 'Total Transaction Revenue'},
             color='district_name', 
             color_discrete_sequence=px.colors.sequential.Magma)

            fig.update_layout(
                title_font_size=16,
                xaxis_title='Districts',
                yaxis_title='Total Revenue',
                xaxis_tickangle=60, 
            )
            bargap=0.05,  # Reduce space between groups (bars)
            bargroupgap=0.1,  # Reduce space between bars within each group
            width=1200, 
            height=600,  
            st.plotly_chart(fig)

        elif sub_dropdown == "Trends Over Time":
            # Fetch data for the selected analysis type
            df = get_transaction_market_analysis(query_type="Trends Over Time")

            col1,col2 = st.columns([2,3])
            # Bar plot to compare seasonal patterns
            with col1:
                fig1 = px.bar(df,
                        x = 'Quarter',
                        y = 'total_revenue',
                        title ='Seasonal Revenue Patterns',
                        labels={'total_revenue': 'Total Revenue'},
                        color_discrete_sequence=px.colors.sequential.Magma)
                fig1.update_layout(
                    title_font_size=16,
                    xaxis_title='Quarter',
                    yaxis_title='Total Revenue',
                    legend_title='States')
                st.plotly_chart(fig1)
                
            with col2:
                fig2 = px.bar(df, 
                    x='years', 
                    y='total_revenue', 
                    color='States', 
                    title='Year-wise Revenue Trends by States',
                    labels={'years': 'Year', 'total_revenue': 'Total Revenue'},
                    color_discrete_sequence=px.colors.sequential.Electric)
                fig2.update_layout(
                    title_font_size=16,
                    xaxis_title='Year',
                    yaxis_title='Total Revenue',
                    legend_title='States')
                st.plotly_chart(fig2)


        elif sub_dropdown == "Market Potential and Strategy Development":
            # Fetch data for the selected analysis type
            df = get_transaction_market_analysis(query_type="Market Potential and Strategy Development")   

            # Scatter plot: Transaction count vs. average transaction value
            plt.figure(figsize=(10,6))
            sns.scatterplot(data=df, x='total_transactions', y='avg_transaction_value', hue="States", size="total_revenue", sizes=(50, 500))
            plt.xlabel('Total Transactions')
            plt.ylabel('Average Transaction Value ($)')
            plt.title('States with High Transactions but Low Average Transaction Values')
            plt.legend(loc='upper left', bbox_to_anchor=(1, 1), title='Total revenue', ncol=2)
            st.pyplot(plt)

    elif dropdown == "User Engagement and Growth Strategy":
        sub_dropdown = st.selectbox(
            "Select Analysis Type", 
            ["User Engagement Analysis","Performance Comparison","Trend Analysis Over Time","Identifying High-Value Markets"]
        ) 

        if sub_dropdown == "User Engagement Analysis":
            # Fetch data for the selected analysis type
            df = get_user_growth_analysis(query_type="User Engagement Analysis")
            col1,col2 = st.columns(2)
            with col1:
                plt.figure(figsize=(10,6))
                sns.barplot(x='States', y='total_registered_users', data=df, palette='viridis')
                plt.title('Total Registered Users by State and District')
                plt.xlabel('States')
                plt.ylabel('Total Registered Users')
                plt.xticks(rotation=60, ha ='right')
                st.pyplot(plt) 

            with col2:
                sns.barplot(x='States', y='avg_appopens', data=df, palette='coolwarm')
                plt.title('Average App Opens per User by State', fontsize=16)
                plt.xlabel('States', fontsize=12)
                plt.ylabel('Average App Opens per User', fontsize=12)
                plt.xticks(rotation=60, ha ='right')
                st.pyplot(plt)
        
        elif sub_dropdown == "Performance Comparison":
            # Fetch data for the selected analysis type
            df = get_user_growth_analysis(query_type="Performance Comparison")
            col1,col2 = st.columns(2)
            with col1:
                fig = px.scatter(
                    df, 
                    x='total_registered_user', 
                    y='total_appopens', 
                    color_continuous_scale=px.colors.sequential.Plasma,
                    title='Registered Users vs App Opens',
                    labels={'total_registered_user': 'Total Registered Users', 'total_appopens': 'Total App Opens'}
                )
                fig.update_layout(
                    title_font_size=20,
                    xaxis_title="Total Registered Users",
                    yaxis_title="Total App Opens"
                )
                st.plotly_chart(fig)

        elif sub_dropdown == "Trend Analysis Over Time":
            df = get_user_growth_analysis(query_type="Trend Analysis Over Time") 
            col1,col2 = st.columns([2,3])
            with col1:
                plt.figure(figsize=(12, 6))
                sns.lineplot(x='Quarter', y='total_user', data= df, marker='o', label='Registered Users')
                sns.lineplot(x='Quarter', y='total_appopens', data=df, marker='o', label='App Opens')
                plt.title('User Registration and App Open Trends Over Quarters')
                plt.xlabel('Quarter')
                plt.ylabel("Total Users and Total App Opens")
                plt.xticks(rotation=45)
                plt.legend()
                plt.grid()
                st.pyplot(plt)
            with col2:
                fig = px.bar(
                    df, 
                    x='Quarter', 
                    y='total_appopens', 
                    color='States',
                    title='Growth Trends in App Opens by State',
                    labels={'total_appopens': 'Total App Opens'},
                    color_discrete_sequence=px.colors.sequential.RdBu
                )

                # Customize layout for better appearance
                fig.update_layout(
                    title_font_size=16,
                    xaxis_title="Quarter",
                    yaxis_title="Total App Opens"
                )

                # Display the plot in Streamlit
                st.plotly_chart(fig)

        elif sub_dropdown == "Identifying High-Value Markets":
             df = get_user_growth_analysis(query_type="Identifying High-Value Markets") 
             col1,col2 = st.columns(2)
             with col1:
                plt.figure(figsize=(14,6))
                # Create the bar plot for total registered users
                sns.barplot(
                        x='States',
                        y='total_registered_user',
                        data=df,
                        palette='viridis',
                )

                    # Add app open ratio as a line plot
                sns.lineplot(
                        x='States',
                        y='app_open_ratio',
                        data=df,
                        marker='o',
                        color='red',
                        label='App Open Ratio'
                )
                plt.title('Total Registered Users and App Open Ratios by States')
                plt.xlabel('States')
                plt.ylabel('Total Registered Users')
                plt.xticks(rotation=45, ha='right')

                # Adding a second y-axis for app open ratio (same plot, different scale)
                plt.twinx()
                plt.ylabel('App Open Ratio', color='red')
                plt.tick_params(axis='y', labelcolor='red')
                st.pyplot(plt)
                    
    elif dropdown == "User Registration Analysis":
        sub_dropdown = st.selectbox(
            "Select Analysis Type", 
            ["Identifying Top 10 States","Analyze fluctuations in user registration across different quarters and states","District Performance Evaluation","Pin Code Insights","Comparative Analysis"]
        )
       
        if sub_dropdown == "Identifying Top 10 States":
         df = get_user_registration_analysis(query_type="Identifying Top 10 States")
         col1,col2= st.columns(2)
         with col1:
            fig = px.bar(df, 
             x="States", 
             y="highest_registered_users", 
             color="years",
             title="Top 10 Highest Registered Users by States, Year, and Quarter",
             labels={"highest_registered_users": "Registered Users", "States": "State"},
             barmode="group")
            
            fig.update_layout(
                 title_font_size=20,
                 xaxis_title="States",
                 yaxis_title="Total Registered Users"
            )
            st.plotly_chart(fig)
        
        elif sub_dropdown == "Analyze fluctuations in user registration across different quarters and states":
            df = get_user_registration_analysis(query_type="Analyze fluctuations in user registration across different quarters and states")
            
            col1, col2 = st.columns(2)  # Creates two columns

            fig= px.bar(
                    df, 
                    x="Quarter", 
                    y="total_registered_users", 
                    color="States", 
                    title="Total Registered Users by Quarter and State with Changes",
                    labels={"total_registered_users": "Total Registered Users", "Quarter": "Quarter"},
                    color_discrete_sequence=px.colors.qualitative.Set2,
                    barmode="group"
                )
            st.plotly_chart(fig)
                # Add the change_from_previous_quarter as a separate line
            fig2 = px.bar(
                                df, 
                                x="Quarter", 
                                y="change_from_previous_quarter", 
                                color="States", 
                                barmode= 'group',
                                title="Change in Registered Users Across Previous Quarters and States",
                                labels={"change_from_previous_quarter": "Change from Previous Quarter"},
                                color_discrete_sequence=px.colors.qualitative.Set2,
                            )
            fig.update_layout(
                    xaxis_title="Quarter",
                    yaxis_title="Total Registered Users",
                    legend_title="States",
                    template="plotly_dark",
                    width=1200,
                    height=600,
                )
            st.plotly_chart(fig2)

                
        elif sub_dropdown == 'District Performance Evaluation':
            df = get_user_registration_analysis(query_type="District Performance Evaluation")
            fig = px.bar(df, 
                            x="district_name", 
                            y="registered_users", 
                            color="States", 
                            title="Top 10 Registered Users by Districts",
                            labels={"registered_users": "Total registered Users", "district_name": "District"})
            fig.update_layout(
                xaxis_title = 'Districts',
                yaxis_title = 'Total registered Users'
            )
            st.plotly_chart(fig)

        elif sub_dropdown == 'Pin Code Insights':
            df = get_user_registration_analysis(query_type="Pin Code Insights")
            # Set the plot size
            plt.figure(figsize=(10, 6))
            sns.barplot(x='pincode', y='user_registrations', hue ='States', data=df, palette='Set2')

            plt.title('Top 10 Pin Codes with Highest User Registrations', fontsize=16)
            plt.xlabel('Pincode', fontsize=12)
            plt.ylabel('User Registrations', fontsize=12)
            plt.legend(ncol=2)
            plt.xticks(rotation=45) 
            st.pyplot(plt)
        elif sub_dropdown == 'Comparative Analysis':
            df = get_user_registration_analysis(query_type="Comparative Analysis")
            # Create an interactive bar plot using Plotly
            fig = px.bar(
                        df, 
                        x='States',
                        y='total_registered_users',
                        color='district_name', 
                        title='Districts with the Highest Registered Users in Each State', 
                        labels={'total_registered_users': 'Total Registered Users', 'district_name': 'District Name','States':'State'},
                        color_discrete_sequence=px.colors.qualitative.Set2
                    )

            fig.update_layout(
                xaxis_title='States',
                yaxis_title='Total Registered Users',
                barmode='stack',
                xaxis_tickangle=-60,
                bargroupgap=0.04,
                bargap=0.1,
                width=2500
            )
            st.plotly_chart(fig)
