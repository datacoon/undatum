## ADDED Requirements

### Requirement: Plot Command
The system SHALL provide a `plot` command for generating data visualizations.

#### Scenario: Generate histogram for numerical field
- **WHEN** user runs `undatum plot data.csv --field age --type histogram --output age_dist.png`
- **THEN** the system SHALL read the data file
- **AND** detect that `age` is a numerical field
- **AND** generate a histogram showing the distribution of age values
- **AND** save the plot to `age_dist.png`

#### Scenario: Generate bar chart for categorical field
- **WHEN** user runs `undatum plot data.csv --field status --type bar`
- **THEN** the system SHALL read the data file
- **AND** detect that `status` is a categorical field
- **AND** generate a bar chart showing frequency of each status value
- **AND** display the plot

#### Scenario: Generate scatter plot for two fields
- **WHEN** user runs `undatum plot data.csv --field x,y --type scatter --output scatter.png`
- **THEN** the system SHALL read the data file
- **AND** generate a scatter plot with x and y fields
- **AND** save the plot to `scatter.png`

#### Scenario: Generate multiple histograms
- **WHEN** user runs `undatum plot data.csv --field age,income,score --type histogram`
- **THEN** the system SHALL generate multiple histogram subplots
- **AND** display all histograms in a single figure

### Requirement: Plot Types
The system SHALL support multiple plot types for different analysis needs.

#### Scenario: Histogram plot
- **WHEN** user requests histogram plot for numerical field
- **THEN** the system SHALL generate a histogram showing value distribution
- **AND** automatically determine appropriate bin count
- **AND** display frequency on y-axis

#### Scenario: Bar chart plot
- **WHEN** user requests bar chart for categorical field
- **THEN** the system SHALL generate a bar chart showing category frequencies
- **AND** sort categories by frequency (optional)
- **AND** display count on y-axis

#### Scenario: Scatter plot
- **WHEN** user requests scatter plot with two numerical fields
- **THEN** the system SHALL generate a scatter plot showing relationship
- **AND** use first field for x-axis and second for y-axis
- **AND** display data points

#### Scenario: Line plot
- **WHEN** user requests line plot for time series data
- **THEN** the system SHALL generate a line plot
- **AND** connect data points with lines
- **AND** support multiple series

### Requirement: Output Formats
Plot output SHALL support multiple formats for different use cases.

#### Scenario: PNG output
- **WHEN** user specifies `--output plot.png`
- **THEN** the system SHALL save plot as PNG image
- **AND** use default resolution (100 DPI)

#### Scenario: SVG output
- **WHEN** user specifies `--output plot.svg`
- **THEN** the system SHALL save plot as SVG vector image
- **AND** enable scalable output

#### Scenario: PDF output
- **WHEN** user specifies `--output plot.pdf`
- **THEN** the system SHALL save plot as PDF document
- **AND** enable print-ready output

#### Scenario: Display output
- **WHEN** user does not specify output file
- **THEN** the system SHALL display plot in interactive window
- **AND** allow user to interact with plot

### Requirement: Plot Customization
Plots SHALL support customization of appearance and labels.

#### Scenario: Custom title and labels
- **WHEN** user runs `undatum plot data.csv --field age --title "Age Distribution" --xlabel "Age (years)" --ylabel "Frequency"`
- **THEN** the system SHALL apply custom title and axis labels
- **AND** display them on the plot

#### Scenario: Custom colors
- **WHEN** user runs `undatum plot data.csv --field status --color viridis`
- **THEN** the system SHALL apply specified color scheme
- **AND** use it for plot elements

#### Scenario: Custom figure size
- **WHEN** user runs `undatum plot data.csv --field age --width 12 --height 8`
- **THEN** the system SHALL create plot with specified dimensions
- **AND** maintain aspect ratio if needed

### Requirement: Integration with Stats
The plot command SHALL integrate with statistics computation when beneficial.

#### Scenario: Plot from stats data
- **WHEN** user runs `undatum plot data.csv --field age --use-stats`
- **THEN** the system SHALL use pre-computed statistics if available
- **AND** optimize plot generation using statistical data
- **AND** display statistical annotations on plot

### Requirement: Field Type Detection
The system SHALL automatically detect field types and suggest appropriate plot types.

#### Scenario: Auto-detect numerical field
- **WHEN** user runs `undatum plot data.csv --field age`
- **THEN** the system SHALL detect that `age` is numerical
- **AND** suggest histogram as default plot type
- **AND** generate appropriate plot

#### Scenario: Auto-detect categorical field
- **WHEN** user runs `undatum plot data.csv --field status`
- **THEN** the system SHALL detect that `status` is categorical
- **AND** suggest bar chart as default plot type
- **AND** generate appropriate plot
