module.exports = {
    apps : [{
      name: "supair",
      script: "./index.js",
      error_file: './logs/err.log',
      out_file: './logs/out.log',
      log_file: './logs/combined.log',
      time: true
      /* env: {
        NODE_ENV: "development",
      },
      env_production: {
        NODE_ENV: "production",
      } */
    }]
  }