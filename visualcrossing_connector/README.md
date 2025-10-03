# Visual Crossing Weather Connector for Looker Studio

A Google Apps Script-based Looker Studio Community Connector that brings Visual Crossing weather data into Looker Studio. This connector supports both single location weather data and batch processing for multiple store locations via Google Sheets integration.

## 🌤️ Features

- **Daily & Hourly Weather Data**: Access comprehensive weather metrics including temperature, humidity, precipitation, wind speed, and more
- **Single Location Mode**: Get weather data for a specific location
- **Batch Store Mode**: Process multiple store locations from a Google Sheet
- **Flexible Date Ranges**: Historical and forecast weather data
- **Multiple Units**: Support for metric, imperial, and scientific units
- **Caching**: Built-in response caching to optimize API usage
- **Error Handling**: Comprehensive error handling and debugging

## 📊 Available Weather Metrics

### Temperature Metrics
- `temp` - Average temperature
- `tempmax` - Maximum temperature
- `tempmin` - Minimum temperature
- `feelslike` - Feels like temperature
- `feelslikemax` - Maximum feels like temperature
- `feelslikemin` - Minimum feels like temperature

### Atmospheric Metrics
- `humidity` - Relative humidity
- `dew` - Dew point
- `pressure` - Atmospheric pressure
- `cloudcover` - Cloud cover percentage
- `visibility` - Visibility distance

### Precipitation Metrics
- `precip` - Precipitation amount
- `precipprob` - Precipitation probability
- `precipcover` - Precipitation cover
- `snow` - Snow amount
- `snowdepth` - Snow depth

### Wind Metrics
- `windspeed` - Wind speed
- `winddir` - Wind direction
- `windgust` - Wind gust speed

### Solar Metrics
- `solarradiation` - Solar radiation
- `solarenergy` - Solar energy
- `uvindex` - UV index

### Time Dimensions
- `datetime` - Date and time
- `datetimeEpoch` - Unix timestamp
- `location_key` - Location identifier

## 🚀 Quick Start

### Prerequisites

1. **Visual Crossing API Key**: Get your free API key from [Visual Crossing Weather](https://www.visualcrossing.com/weather-api)
2. **Google Apps Script**: Access to Google Apps Script platform
3. **Looker Studio**: Access to Google Looker Studio (formerly Data Studio)

### Installation

1. **Clone or Download** this repository
2. **Open Google Apps Script** and create a new project
3. **Copy the contents** of `Code.js` into your Apps Script editor
4. **Copy the contents** of `appsscript.json` into your Apps Script manifest
5. **Deploy** the connector using `clasp` or the Apps Script editor

### Using Clasp (Recommended)

```bash
# Install clasp globally
npm install -g @google/clasp

# Login to Google
clasp login

# Create a new Apps Script project
clasp create --type standalone --title "Visual Crossing Weather Connector"

# Copy files to your project directory
# Then push to Apps Script
clasp push
```

## 🔧 Configuration

### Single Location Mode

1. **API Key**: Enter your Visual Crossing API key
2. **Location**: Specify the location (e.g., "New York, NY", "London, UK")
3. **Data Type**: Choose "daily" or "hourly"
4. **Units**: Select "us" (imperial), "metric", or "uk"
5. **Date Range**: Set your desired date range

### Store Batch Mode

1. **API Key**: Enter your Visual Crossing API key
2. **Google Sheet**: Provide the Google Sheet ID containing store locations
3. **Sheet Tab**: Specify the tab name (default: "Stores")
4. **Data Type**: Choose "daily" or "hourly"
5. **Units**: Select your preferred unit system

## 📋 Google Sheet Format for Store Join

When using the store batch mode, your Google Sheet should have the following columns:

| store_id | address | city | state | country |
|----------|---------|------|-------|---------|
| 001 | 123 Main St | New York | NY | USA |
| 002 | 456 Oak Ave | Los Angeles | CA | USA |
| 003 | 789 Pine St | Chicago | IL | USA |

### Required Columns:
- **store_id**: Unique identifier for each store
- **address**: Street address
- **city**: City name
- **state**: State/province (optional)
- **country**: Country name

### Optional Columns:
- **latitude**: Latitude coordinate (if provided, will be used for more accurate weather data)
- **longitude**: Longitude coordinate (if provided, will be used for more accurate weather data)

## 🔗 Store Join Process

The connector supports joining existing Looker Studio data with weather data through the following methods:

### Method 1: Google Sheet Integration
1. **Prepare your store data** in a Google Sheet with the required columns
2. **Configure the connector** to use "Store Batch Mode"
3. **Provide the Sheet ID** and tab name
4. **The connector will automatically** fetch weather data for all store locations
5. **Use the store_id** to join with your existing Looker Studio data

### Method 2: Manual Location Entry
1. **Use Single Location Mode** for individual locations
2. **Manually enter locations** that match your existing data
3. **Use location names** that can be matched with your existing datasets

### Method 3: Hybrid Approach
1. **Use Store Batch Mode** for bulk weather data
2. **Combine with Single Location Mode** for additional locations
3. **Use location_key field** to join datasets in Looker Studio

## 📈 Usage in Looker Studio

### Creating a Report

1. **Add the Connector**: In Looker Studio, click "Create" → "Data Source"
2. **Search for "Visual Crossing"** or use the connector URL
3. **Configure the connector** with your API key and settings
4. **Authorize** the connector
5. **Select your metrics** and dimensions
6. **Create your visualization**

### Recommended Chart Types

- **Time Series**: For temperature trends over time
- **Table**: For detailed daily/hourly data
- **Geographic**: For weather data across multiple locations
- **Scorecard**: For current weather conditions

### Best Practices

1. **Use Date Dimension**: Always include the date dimension to see time-series data
2. **Set Proper Aggregation**: Use "Average" for temperature, "Sum" for precipitation
3. **Filter by Location**: Use location filters to focus on specific areas
4. **Cache Data**: The connector includes caching to optimize performance

## 🛠️ Development

### Project Structure

```
VisualCrossing/
├── Code.js              # Main connector code
├── appsscript.json       # Apps Script manifest
└── README.md            # This documentation
```

### Key Functions

- `getAuthType()`: Defines authentication (currently NONE)
- `getConfig()`: Configuration UI for the connector
- `getSchema()`: Defines available fields and metrics
- `getData()`: Fetches and processes weather data
- `fetchVCSingle_()`: Fetches weather for single location
- `fetchVCBatch_()`: Fetches weather for multiple locations
- `loadStoresFromSheet_()`: Loads store data from Google Sheet

### Error Handling

The connector includes comprehensive error handling:
- **API Errors**: Detailed error messages for API failures
- **Authentication**: Clear messages for API key issues
- **Data Validation**: Checks for required fields and valid data
- **Caching**: Prevents cache overflow errors

## 🔍 Debugging

### Enable Debug Logging

The connector includes extensive debug logging. To view logs:

1. **Open Apps Script Editor**
2. **Go to Executions** in the left sidebar
3. **View execution logs** for detailed debugging information

### Common Issues

1. **"Community Connector Error"**: Usually indicates API key issues or malformed requests
2. **"Argument too large"**: Cache size limit exceeded (connector handles this automatically)
3. **"Missing data"**: Check date range and location format
4. **"Location not found"**: Verify location format (city, state, country)

## 📚 API Documentation

### Visual Crossing Weather API

- **Documentation**: [Visual Crossing Weather API](https://www.visualcrossing.com/resources/documentation/weather-api/)
- **Rate Limits**: Free tier includes 1000 requests/day
- **Data Coverage**: Global weather data with 15+ years of historical data
- **Update Frequency**: Real-time data with hourly updates

### Looker Studio Connectors

- **Documentation**: [Looker Studio Connectors](https://developers.google.com/looker-studio/connector/build)
- **Community Connectors**: [Community Connector Reference](https://developers.google.com/looker-studio/connector/reference)

## 🤝 Contributing

We welcome contributions! Please:

1. **Fork the repository**
2. **Create a feature branch**
3. **Make your changes**
4. **Test thoroughly**
5. **Submit a pull request**

### Development Setup

```bash
# Clone the repository
git clone <repository-url>
cd VisualCrossing

# Install clasp
npm install -g @google/clasp

# Login to Google
clasp login

# Create a new project
clasp create --type standalone --title "Visual Crossing Weather Connector"

# Push changes
clasp push
```

## 📄 License

This project is licensed under the Apache License 2.0. See the [LICENSE](LICENSE) file for details.

## 🆘 Support

- **Documentation**: [Visual Crossing Weather API Docs](https://www.visualcrossing.com/resources/documentation/weather-api/)
- **Support**: [Visual Crossing Support](https://www.visualcrossing.com/support)
- **Issues**: Report issues through GitHub Issues
- **Community**: [Looker Studio Community](https://developers.google.com/looker-studio/connector/build)

## 🎯 Roadmap

- [ ] **Enhanced Caching**: Implement smarter caching strategies
- [ ] **More Data Sources**: Support for additional weather data providers
- [ ] **Advanced Filtering**: Date range and location filtering in the connector
- [ ] **Custom Metrics**: Allow users to define custom weather metrics
- [ ] **Real-time Updates**: Support for real-time weather data
- [ ] **Geographic Visualization**: Enhanced mapping capabilities

## 📊 Example Use Cases

### Retail Analytics
- **Store Performance**: Correlate weather with sales data
- **Seasonal Trends**: Analyze weather impact on product sales
- **Location Planning**: Use weather data for new store locations

### Agriculture
- **Crop Planning**: Use historical weather data for planting schedules
- **Yield Analysis**: Correlate weather with crop yields
- **Risk Assessment**: Monitor weather conditions for crop protection

### Energy Management
- **Demand Forecasting**: Use weather data for energy demand prediction
- **Renewable Energy**: Correlate weather with solar/wind energy production
- **Efficiency Optimization**: Optimize energy usage based on weather conditions

### Transportation
- **Route Planning**: Consider weather conditions for logistics
- **Safety Analysis**: Monitor weather impact on transportation safety
- **Delay Prediction**: Use weather data to predict transportation delays

---

**Built with ❤️ by Visual Crossing and the Looker Studio Community**
