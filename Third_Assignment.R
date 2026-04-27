# =========================================================
# PROVENANCE MONITORING DASHBOARD (ULTIMATE VERSION)
# =========================================================

# =========================================================
# 1. LOAD LIBRARIES
# =========================================================
# Here I load all the libraries I need for the dashboard:
# shiny -> to build the web app
# mongolite -> to connect to MongoDB
# dplyr & purrr -> for data manipulation
# plotly -> interactive plots
# DT -> tables
# jsonlite & listviewer -> to handle JSON data

library(shiny)
library(mongolite)
library(dplyr)
library(purrr)
library(plotly)
library(DT)
library(jsonlite)
library(listviewer)
library(visNetwork)
library(shinyWidgets)
# =========================================================
# 2. CONNECT TO MONGODB
# =========================================================
# Here I define the MongoDB connection URL with credentials and cluster info
mongo_url <- "mongodb://carolinal_db_user:zanahorita@ac-n86xomm-shard-00-00.mr5yolt.mongodb.net:27017,ac-n86xomm-shard-00-01.mr5yolt.mongodb.net:27017,ac-n86xomm-shard-00-02.mr5yolt.mongodb.net:27017/provenanceDB?ssl=true&replicaSet=atlas-xoy4rq-shard-0&authSource=admin&tls=true&tlsAllowInvalidCertificates=true&tlsAllowInvalidHostnames=true"

# I connect to the "logs" collection inside the database
collection <- mongo("logs", url = mongo_url)

# Then I retrieve all the data from MongoDB
data_raw <- collection$find()

# =========================================================
# 3. DATA TRANSFORMATION
# =========================================================
# In this step I clean and transform the raw data to make it usable for analysis

data_clean <- data_raw %>%
  mutate(
    # Rename some fields for clarity
    sample = label,
    node   = executionNode,
    
    # Convert timestamps from string to POSIXct format
    startTime = as.POSIXct(startTime, format="%Y-%m-%dT%H:%M:%SZ", tz="UTC"),
    endTime   = as.POSIXct(endTime, format="%Y-%m-%dT%H:%M:%SZ", tz="UTC"),
    
    # Compute duration of each run in seconds
    duration  = as.numeric(difftime(endTime, startTime, units = "secs")),
    
    # Extract status values from nested JSON fields
    sha_status   = map_chr(generated, ~ .x$status[1]),
    seqfu_status = map_chr(generated, ~ .x$status[2]),
    
    # Extract total size (in bytes) and convert to numeric
    totalSizeBytes = as.numeric(map_chr(generated, ~ .x$totalSizeBytes[3])),
    
    # Convert size from bytes to GB
    size_GB = round(totalSizeBytes / 1e9, 2),
    
    # Extract user name from nested structure
    user = map_chr(wasAssociatedWith, function(x) {
      person_row <- x[x$`@type` == "Person", ]
      if(nrow(person_row) > 0) person_row$label[1] else NA
    }),
    
    # Clean user string (remove prefix text)
    user = sub("Usuari executor: ", "", user),
    
    # Define overall status: OK only if both processes are OK
    status = ifelse(sha_status == "OK" & seqfu_status == "OK", "OK", "FAIL"),
    
    # Compute efficiency (time per GB processed)
    efficiency = duration / size_GB,
    
    # Compute throughput (GB processed per second)
    throughput = size_GB / duration,
    
    # Categorize runs based on size
    size_category = case_when(
      size_GB < 1 ~ "small",
      size_GB < 3 ~ "medium",
      TRUE ~ "large"
    ),
    
    # Extract hour (for time-based analysis)
    hour = format(startTime, "%Y-%m-%d %H:00")
  )

# Count how many runs have dependencies
dependencies <- sum(!is.na(data_raw$wasInformedBy))

# =========================================================
# 4. UI
# =========================================================
# Here I define the user interface of the Shiny dashboard

ui <- fluidPage(
  
  # Title of the app
  titlePanel("PROV-O Dashboard"),
  
  sidebarLayout(
    
    sidebarPanel(
      # Filters section
      h4("Filters"),
      
      # Filter by execution node
      checkboxGroupButtons(
        inputId = "node",
        label = "Execution Node:",
        choices = unique(data_clean$node),
        selected = unique(data_clean$node),
        checkIcon = list(yes = icon("check")),
        status = "primary"
        ),  
      
      # Filter by user
      checkboxGroupButtons(
        inputId = "user",
        label = "User:",
        choices = unique(data_clean$user),
        selected = unique(data_clean$user),
        checkIcon = list(yes = icon("check")),
        status = "info"
      ),
      
      # Filter by status (OK / FAIL)
      checkboxGroupButtons(
        inputId = "status",
        label = "Status:",
        choices = c("OK", "FAIL"),
        selected = c("OK", "FAIL"),
        checkIcon = list(yes = icon("check"))
      )
    ),
    mainPanel(
      
      # KPI cards row
      fluidRow(
        column(3, div(style="background:#e8f5e9;padding:15px;border-radius:12px",
                      h4("Runs"), h2(textOutput("kpi_runs")))),
        
        column(3, div(style="background:#ffebee;padding:15px;border-radius:12px",
                      h4("Failure Rate"), h2(textOutput("kpi_fail")))),
        
        column(3, div(style="background:#e3f2fd;padding:15px;border-radius:12px",
                      h4("Avg Duration"), h2(textOutput("kpi_duration")))),
        
        column(3, div(style="background:#f3e5f5;padding:15px;border-radius:12px",
                      h4("Total GB"), h2(textOutput("kpi_gb"))))
      ),
      
      br(),
      
      # Additional KPIs
      fluidRow(
        column(3, div(style="background:#fff3e0;padding:15px;border-radius:12px",
                      h4("Dependencies"), h2(textOutput("kpi_dep")))),
        
        column(3, div(style="background:#e0f7fa;padding:15px;border-radius:12px",
                      h4("Correlation"), h2(textOutput("kpi_corr"))))
      ),
      
      br(),
      
      # Alerts section
      h4(textOutput("alerts")),
      
      hr(),
      
      # Tabs for different analyses
      tabsetPanel(
        
        # Overview tab with general plots
        tabPanel("Overview",
                 p("Legend:",
                   span("● OK", style="color:#2ecc71;font-weight:bold;"),
                   span("● FAIL", style="color:#e74c3c;font-weight:bold;margin-left:10px;")
                 ),
                 plotlyOutput("scatter"),
                 plotlyOutput("fail_plot"),
                 plotlyOutput("size_plot")
        ),
        
        # Node-based analysis
        tabPanel("Node Analysis",
                 plotlyOutput("boxplot"),
                 plotlyOutput("node_plot"),
                 plotlyOutput("efficiency_plot")
        ),
        
        # Time-based analysis
        tabPanel("Temporal Analysis",
                 plotlyOutput("time_plot")
        ),
        
        # User-based analysis
        tabPanel("User Analysis",
                 plotlyOutput("user_plot")
        ),
        
        # Data exploration tab
        tabPanel("Data",
                 DTOutput("table"),
                 br(),
                 fluidRow(
                   column(4, uiOutput("activity_card")),
                   column(8,
                          div(style="background:#ffffff;padding:15px;border-radius:12px;
           box-shadow:0px 2px 6px rgba(0,0,0,0.1);
           height:auto;",
    
    # Provenance graph visualization
    h4("Provenance Graph"),
    visNetworkOutput("prov_graph", height = "250px"),
    
    br(),
    
    # Collapsible JSON viewer
    tags$details(
      tags$summary("Show raw JSON"),
      div(
        style="max-height:250px;overflow-y:auto;overflow-x:auto;
           background:#f8f9fa;padding:10px;border-radius:8px;",
        verbatimTextOutput("json")
      )
    )
)
                   )
                 ),
                 br(),
                 
                 # Show agents and generated entities
                 fluidRow(
                   column(6, h4("Agents"), uiOutput("agents")),
                   column(6, h4("Generated Entities"), uiOutput("entities"))
                 )
        )
      )
    )
  )
)


# =========================================================
# 5. SERVER
# =========================================================
# Here I define the server logic of the Shiny app.
# This is where all the reactive behavior, calculations, and plots are generated.

server <- function(input, output, session) {
  
  # Reactive dataset: this updates automatically when filters change
  # It filters the cleaned data based on node, user, and status selections
  df <- reactive({
    data_clean %>%
      filter(
        node %in% input$node,
        user %in% input$user,
        status %in% input$status
      )
  })
  
  # KPI: total number of runs
  output$kpi_runs <- renderText({ nrow(df()) })
  
  # KPI: failure rate (percentage of FAIL executions)
  output$kpi_fail <- renderText({
    paste0(round(mean(df()$status=="FAIL")*100,2), "%")
  })
  
  # KPI: average duration in seconds
  output$kpi_duration <- renderText({
    paste0(round(mean(df()$duration),2), " s")
  })
  
  # KPI: total processed data in GB
  output$kpi_gb <- renderText({
    paste0(round(sum(df()$size_GB),2), " GB")
  })
  
  # KPI: number of dependencies (precomputed globally)
  output$kpi_dep <- renderText({ dependencies })
  
  # KPI: correlation between data size and execution time
  output$kpi_corr <- renderText({
    round(cor(df()$size_GB, df()$duration),2)
  })
  
  # Alert message depending on failure rate
  output$alerts <- renderText({
    if(mean(df()$status=="FAIL") > 0.1){
      "High failure rate detected"
    } else {
      "System OK"
    }
  })
  
  # Scatter plot: relationship between data size and execution time
  output$scatter <- renderPlotly({
    plot_ly(df(), x=~size_GB, y=~duration, type="scatter", mode="markers",
            color=~status, colors=c("OK"="#2ecc71","FAIL"="#e74c3c"),
            symbol=~node, text=~paste(sample,node), hoverinfo="text") %>%
      layout(xaxis=list(title="Data Size (GB)"),
             yaxis=list(title="Execution Time (seconds)"))
  })
  
  # Bar plot: number of executions by status (OK vs FAIL)
  output$fail_plot <- renderPlotly({
    plot_ly(df() %>% count(status), x=~status, y=~n, type="bar",
            color=~status, colors=c("OK"="#2ecc71","FAIL"="#e74c3c")) %>%
      layout(xaxis=list(title="Execution Status"),
             yaxis=list(title="Number of Executions"))
  })
  
  # Bar plot: distribution of runs by size category (small, medium, large)
  output$size_plot <- renderPlotly({
    plot_ly(df() %>% count(size_category), x=~size_category, y=~n, type="bar") %>%
      layout(xaxis=list(title="Data Size Category"),
             yaxis=list(title="Number of Executions"))
  })
  
  # Bar plot: average execution time per node
  output$node_plot <- renderPlotly({
    plot_ly(df() %>% group_by(node) %>% summarise(avg=mean(duration)),
            x=~node, y=~avg, type="bar", color=~node,
            colors=c("#A8DADC","#FFD6A5","#BDB2FF","#FFC6FF")) %>%
      layout(xaxis=list(title="Execution Node"),
             yaxis=list(title="Average Execution Time (seconds)"))
  })
  
  # Bar plot: efficiency per node (seconds per GB)
  output$efficiency_plot <- renderPlotly({
    plot_ly(df() %>% group_by(node) %>% summarise(eff=mean(efficiency)),
            x=~node, y=~eff, type="bar", color=~node,
            colors=c("#A8DADC","#FFD6A5","#BDB2FF","#FFC6FF")) %>%
      layout(xaxis=list(title="Execution Node"),
             yaxis=list(title="Efficiency (seconds per GB)"))
  })
  
  # Time series plot: number of executions over time (grouped by hour)
  output$time_plot <- renderPlotly({
    plot_ly(df() %>% group_by(hour) %>% summarise(runs=n()),
            x=~hour, y=~runs, type="scatter", mode="lines+markers") %>%
      layout(xaxis=list(title="Time (Hourly Intervals)", tickangle=-45),
             yaxis=list(title="Number of Executions"))
  })
  
  # Bar plot: number of executions per user
  output$user_plot <- renderPlotly({
    plot_ly(df() %>% group_by(user) %>% summarise(runs=n()),
            x=~user, y=~runs, type="bar", color=~user,
            colors=c("#FFADAD","#CAFFBF","#9BF6FF","#FDFFB6")) %>%
      layout(xaxis=list(title="User"),
             yaxis=list(title="Number of Executions"))
  })
  
  # Boxplot: distribution of execution times per node
  output$boxplot <- renderPlotly({
    plot_ly(df(), x=~node, y=~duration, type="box", color=~node,
            colors=c("#A8DADC","#FFD6A5","#BDB2FF","#FFC6FF")) %>%
      layout(xaxis=list(title="Execution Node"),
             yaxis=list(title="Execution Time Distribution (seconds)"))
  })
  
  # Data table: shows the filtered dataset
  output$table <- renderDT({
    datatable(df(), selection="single")
  })

    # This UI element shows detailed information about the selected activity (row in the table)
  output$activity_card <- renderUI({
    
    # If no row is selected, do not show anything
    if (is.null(input$table_rows_selected)) return(NULL)
    
    # Get the selected row from the filtered dataset
    row <- df()[input$table_rows_selected, ]
    
    # Define color depending on status (green for OK, red for FAIL)
    status_color <- ifelse(row$status=="OK","#2ecc71","#e74c3c")
    
    # Create a card-like layout displaying activity information
    div(style="background:#f5f5f5;padding:15px;border-radius:12px",
        h4("Activity"),
        p(strong("Sample:"), row$sample),
        p(strong("Node:"), row$node),
        p(strong("User:"), row$user),
        p(strong("Duration:"), paste0(round(row$duration,2)," s")),
        p(strong("Size:"), paste0(row$size_GB," GB")),
        
        # Status label with dynamic color
        span(row$status,
             style=paste0("background:",status_color,";color:white;padding:5px 10px;border-radius:8px"))
    )
  })
  

  # This section displays the agents associated with the selected execution
  output$agents <- renderUI({
    
    # If no row is selected, do not display anything
    if (is.null(input$table_rows_selected)) return(NULL)
    
    # Get selected row from filtered data
    row <- df()[input$table_rows_selected, ]
    
    # Find the corresponding full record in the raw dataset
    selected <- data_raw[data_raw$label == row$sample, ][1, ]
    
    # Extract agents (wasAssociatedWith field)
    agents <- selected$wasAssociatedWith[[1]]
    
    # Create a list of UI elements (one per agent)
    tagList(lapply(1:nrow(agents), function(i){
      
      # Extract type of agent (e.g., Person or System)
      type <- agents$`@type`[i]
      
      # Assign color depending on type
      color <- ifelse(type == "Person", "#FFD166", "#06D6A0")
      
      # Create a card for each agent
      div(style=paste0(
        "background:", color, ";
         padding:10px;
         border-radius:10px;
         margin-bottom:10px;
         color:#333;"
      ),
          strong(agents$label[i]),
          br(),
          span(paste("Type:", type))
      )
    }))
  })
  

  # This section shows the entities generated during the execution
  output$entities <- renderUI({
    
    # If no row is selected, return nothing
    if (is.null(input$table_rows_selected)) return(NULL)
    
    # Get selected row
    row <- df()[input$table_rows_selected, ]
    
    # Match with original raw data
    selected <- data_raw[data_raw$label == row$sample, ][1, ]
    
    # Extract generated entities
    entities <- selected$generated[[1]]
    
    # Create UI elements for each entity
    tagList(lapply(1:nrow(entities), function(i){
      
      # Extract status of each entity
      status <- entities$status[i]
      
      # Define background color depending on status
      color <- ifelse(status == "OK", "#D4EDDA",
               ifelse(status == "FAIL", "#F8D7DA", "#E3F2FD"))
      
      # Create a card for each generated entity
      div(style=paste0(
        "background:", color, ";
         padding:10px;
         border-radius:10px;
         margin-bottom:10px;"
      ),
          strong(entities$label[i]),
          br(),
          
          # Show value if it exists
          ifelse(!is.null(entities$value[i]), entities$value[i], ""),
          
          # Show status if available
          if(!is.null(status)) span(paste("(", status, ")"))
      )
    }))
  })
  

  # This output displays the raw JSON of the selected execution
  output$json <- renderText({
    
    # If no row is selected, return empty text
    if (is.null(input$table_rows_selected)) return("")
    
    # Get selected row
    row <- df()[input$table_rows_selected, ]
    
    # Match with original dataset
    selected <- data_raw[data_raw$label == row$sample, ][1, ]
    
    # Convert to formatted JSON for visualization
    toJSON(selected, auto_unbox = TRUE, pretty = TRUE)
  })
  

  # This section builds the provenance graph (nodes and edges)
  output$prov_graph <- renderVisNetwork({
    
    # If no row is selected, do not render graph
    if (is.null(input$table_rows_selected)) return(NULL)
    
    # Get selected row
    row <- df()[input$table_rows_selected, ]
    
    # Retrieve full record
    selected <- data_raw[data_raw$label == row$sample, ][1, ]
    
    # Initialize empty data frames for nodes and edges
    nodes <- data.frame(id=character(), label=character(), group=character(), stringsAsFactors=FALSE)
    edges <- data.frame(from=character(), to=character(), label=character(), stringsAsFactors=FALSE)
      # =========================
  # ACTIVITY
  # =========================
  # Here I create the main node representing the current activity (execution)
  activity_id <- as.character(selected$label)
  
  nodes <- bind_rows(nodes, data.frame(
    id = activity_id,
    label = activity_id,
    group = "activity"  # This group will define how the node is styled in the graph
  ))
  

  # =========================
  # AGENTS
  # =========================
  # These are the agents (users or systems) associated with the activity
  agents <- selected$wasAssociatedWith[[1]]
  
  # I check that agents exist and the table is not empty
  if(!is.null(agents) && nrow(agents) > 0){
    for(i in seq_len(nrow(agents))){
      
      # Extract agent identifier (using its label)
      agent_id <- as.character(agents$label[i])
      
      # Add agent as a node in the graph
      nodes <- bind_rows(nodes, data.frame(
        id = agent_id,
        label = agent_id,
        group = "agent"
      ))
      
      # Create an edge from the agent to the activity
      # This represents the PROV relation "wasAssociatedWith"
      edges <- bind_rows(edges, data.frame(
        from = agent_id,
        to = activity_id,
        label = "associatedWith"
      ))
    }
  }
  

  # =========================
  # GENERATED
  # =========================
  # These are the entities produced by the activity
  generated <- selected$generated[[1]]
  
  # Check if generated entities exist
  if(!is.null(generated) && nrow(generated) > 0){
    for(i in seq_len(nrow(generated))){
      
      # Create a unique ID for each generated entity
      ent_id <- paste0("gen_", generated$label[i])
      
      # Add entity as a node
      nodes <- bind_rows(nodes, data.frame(
        id = ent_id,
        label = generated$label[i],
        group = "generated"
      ))
      
      # Create edge from activity to generated entity
      # This follows the PROV relation "wasGeneratedBy"
      edges <- bind_rows(edges, data.frame(
        from = activity_id,
        to = ent_id,
        label = "generated"
      ))
    }
  }
  

  # =========================
  # USED
  # =========================
  # These are the input entities that were used by the activity
  used <- selected$used[[1]]
  
  # Check if used entities exist
  if(!is.null(used) && nrow(used) > 0){
    for(i in seq_len(nrow(used))){
      
      # Create a unique ID for each used entity
      ent_id <- paste0("used_", used$label[i])
      
      # Add entity as a node
      nodes <- bind_rows(nodes, data.frame(
        id = ent_id,
        label = used$label[i],
        group = "used"
      ))
      
      # Create edge from entity to activity
      # This represents the PROV relation "used"
      edges <- bind_rows(edges, data.frame(
        from = ent_id,
        to = activity_id,
        label = "used"
      ))
    }
  }
  

  # =========================
  # DEPENDENCY (PROV CORRECT)
  # =========================
  # This represents dependencies between activities (workflow lineage)
  if(!is.null(selected$wasInformedBy)){
    
    # Extract the ID of the previous activity
    dep_id <- as.character(selected$wasInformedBy$`@id`)
    
    # Add previous activity as a node
    nodes <- bind_rows(nodes, data.frame(
      id = dep_id,
      label = "Previous Activity",
      group = "activity"
    ))
    
    # Create edge showing dependency between activities
    # This corresponds to PROV relation "wasInformedBy"
    edges <- bind_rows(edges, data.frame(
      from = dep_id,
      to = activity_id,
      label = "wasInformedBy"
    ))
  }
  

  # Remove duplicated nodes and edges to keep the graph clean
  nodes <- unique(nodes)
  edges <- unique(edges)
    # Here I build the interactive provenance graph using visNetwork
  visNetwork(nodes, edges) %>%
  
  # Define colors for each type of node (group)
  visGroups(groupname = "activity", color = "#FFD166") %>%
  visGroups(groupname = "agent", color = "#06D6A0") %>%
  visGroups(groupname = "generated", color = "#118AB2") %>%
  visGroups(groupname = "used", color = "#EF476F") %>%
  
  # General node styling
  visNodes(
    shape = "dot",        # All nodes are displayed as circles
    size = 20,            # Fixed node size
    
    # Font configuration for labels
    font = list(
      size = 14,
      vadjust = 15   # Moves the label slightly downward for better readability
    ),
    
    # Limit maximum width of text labels to avoid overflow
    widthConstraint = list(maximum = 80)
  ) %>%
  
  # Edge styling (connections between nodes)
  visEdges(
    arrows = "to",  # Directional arrows
    
    # Smooth curved edges to improve readability
    smooth = list(
      enabled = TRUE,
      type = "curvedCW",
      roundness = 0.2
    )
  ) %>%
  
  # Hierarchical layout (top to bottom)
  visHierarchicalLayout(
    direction = "UD",        # Up to Down layout
    levelSeparation = 200,  # Vertical distance between levels
    nodeSpacing = 180,      # Horizontal spacing between nodes
    treeSpacing = 200       # Space between different branches
  ) %>%
  
  # Physics engine to improve spacing and avoid overlaps
  visPhysics(
    enabled = TRUE,
    solver = "repulsion",
    repulsion = list(
      nodeDistance = 180,   # Distance between nodes
      springLength = 120    # Edge length (spring behavior)
    )
  ) %>%
  
  # Enable highlighting of nearest nodes when hovering
  visOptions(highlightNearest = TRUE)
})


  # This output simply shows a summary of the workflow dependencies
  output$workflow <- renderText({
    paste("Dependencies:", dependencies)
  })

}
# =========================================================
# RUN APP
# =========================================================
# Finally, this line launches the Shiny application
shinyApp(ui, server)
