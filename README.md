# Pi-Qualytics - Enterprise Data Quality Platform

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Snowflake](https://img.shields.io/badge/Snowflake-Ready-29B5E8?logo=snowflake)](https://www.snowflake.com/)
[![Next.js](https://img.shields.io/badge/Next.js-14-black?logo=next.js)](https://nextjs.org/)

**Enterprise-grade Data Quality platform with AI-driven insights, automated profiling, and comprehensive quality checks.**

---

## 🎯 Overview

Pi-Qualytics is a production-ready data quality platform that provides:
- **3-Layer Medallion Architecture** (Bronze, Silver, Gold)
- **65+ Pre-configured DQ Rules** (Completeness, Uniqueness, Validity, Consistency, Freshness, Volume)
- **Automated Scheduling** (Profiling & custom checks)
- **AI-Driven Insights** (Anomaly detection & trend analysis)
- **Real-time Monitoring** (Dashboards & scorecards)

---

## 🚀 Quick Start

### **Prerequisites**
- Node.js 18+ and npm
- Snowflake account with ACCOUNTADMIN role
- Docker (optional, for containerized deployment)

### **Installation**

```bash
# Clone the repository
git clone https://github.com/your-org/pi-qualytics.git
cd pi-qualytics

# Install dependencies
npm install

# Configure environment
cp .env.example .env
# Edit .env with your Snowflake credentials

# Run development server
npm run dev
```

Open [http://localhost:3000](http://localhost:3000) to view the application.

---

## 📁 Project Structure

```
pi-qualytics/
├── app/                    # Next.js app directory (pages, API routes)
├── components/             # React components
├── lib/                    # Utility functions and helpers
├── sql/production/         # Production SQL scripts (12 files)
│   ├── 00_MASTER_SETUP.sql
│   ├── 01-11_*.sql        # Setup scripts
│   ├── README.md          # SQL documentation
│   └── SETUP_GUIDE.md     # Detailed setup instructions
├── docs/                   # Documentation
│   ├── setup/             # Setup guides
│   └── troubleshooting/   # Troubleshooting guides
├── scripts/               # Utility scripts
├── public/                # Static assets
└── README.md              # This file
```

---

## 🏗️ Architecture

### **Application Stack**
- **Frontend**: Next.js 14, React, TailwindCSS, shadcn/ui
- **Backend**: Next.js API Routes, Snowflake Node.js Driver
- **Database**: Snowflake (3-layer architecture)
- **AI**: Gemini API for insights generation

### **Snowflake Architecture**

```
BANKING_DW (Business Data)
├── BRONZE → Raw data (STRING columns)
├── SILVER → Cleansed data (Type-safe with DQ scores)
└── GOLD → Analytics views (Business-ready)

DATA_QUALITY_DB (DQ Framework)
├── DQ_CONFIG → Rules, datasets, schedules
├── DQ_METRICS → Results, profiling, summaries
├── DQ_ENGINE → Stored procedures
└── DB_METRICS → AI observability
```

---

## 📊 Features

### **Data Quality Checks**
- ✅ **Completeness**: Null checks, critical column validation
- ✅ **Uniqueness**: Primary key, composite key validation
- ✅ **Validity**: Email, phone, date format, allowed values
- ✅ **Consistency**: Foreign keys, logical relationships
- ✅ **Freshness**: Load timestamp validation
- ✅ **Volume**: Anomaly detection

### **Automated Workflows**
- ✅ **Scheduled Profiling**: Automated column profiling
- ✅ **Custom Checks**: Rule-based quality validation
- ✅ **AI Insights**: Automatic anomaly & trend detection
- ✅ **Real-time Monitoring**: Live dashboards

### **User Interface**
- ✅ **Interactive Dashboards**: Quality scorecards, trends
- ✅ **Investigation Mode**: AI-powered SQL generation
- ✅ **Scheduling**: Configure automated scans
- ✅ **Alerts**: Real-time quality notifications

---

## 🛠️ Setup

### **1. Snowflake Setup**

Execute the production SQL scripts in order:

```bash
cd sql/production
# Follow instructions in SETUP_GUIDE.md
```

**See [sql/production/SETUP_GUIDE.md](./sql/production/SETUP_GUIDE.md) for detailed instructions.**

### **2. Application Setup**

```bash
# Install dependencies
npm install

# Configure environment variables
cp .env.example .env
# Edit .env with your Snowflake credentials

# Run development server
npm run dev
```

### **3. Docker Deployment (Optional)**

```bash
# Build and run with Docker Compose
docker-compose up -d
```

---

## 📚 Documentation

| Document | Description |
|----------|-------------|
| [SQL Setup Guide](./sql/production/SETUP_GUIDE.md) | Step-by-step Snowflake setup |
| [SQL README](./sql/production/README.md) | SQL scripts overview |
| [Deployment Guide](./docs/setup/DEPLOYMENT.md) | Production deployment |
| [Troubleshooting](./docs/troubleshooting/) | Common issues & fixes |

---

## 🧪 Testing

```bash
# Run tests
npm test

# Run linter
npm run lint

# Type check
npm run type-check
```

---

## 🚢 Deployment

### **Production Deployment**

```bash
# Build for production
npm run build

# Start production server
npm start
```

### **Docker Deployment**

```bash
# Build image
docker build -t pi-qualytics .

# Run container
docker run -p 3000:3000 pi-qualytics
```

**See [docs/setup/DEPLOYMENT.md](./docs/setup/DEPLOYMENT.md) for detailed deployment instructions.**

---

## 🤝 Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 🆘 Support

For issues or questions:
- 📖 Check [Troubleshooting Guides](./docs/troubleshooting/)
- 🐛 Open an [Issue](https://github.com/your-org/pi-qualytics/issues)
- 📧 Contact: support@pi-qualytics.com

---

## 🙏 Acknowledgments

- Built with [Next.js](https://nextjs.org/)
- Powered by [Snowflake](https://www.snowflake.com/)
- UI components from [shadcn/ui](https://ui.shadcn.com/)
- AI insights from [Google Gemini](https://ai.google.dev/)

---

**Version**: 1.0.0  
**Last Updated**: 2026-01-22  
**Maintained By**: Pi-Qualytics Team
