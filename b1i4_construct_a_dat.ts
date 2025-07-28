/**
 * b1i4_construct_a_dat.ts
 * 
 * A data-driven data pipeline generator project
 * 
 * This project generates data pipelines based on user-defined configurations
 * 
 * Configuration options:
 *  - pipeline_name: string (required)
 *  - data_source: string (required)
 *  - data_transformations: DataTransformation[] (optional)
 *  - data_sink: string (required)
 * 
 * DataTransformation interface:
 *  - type: string (required)
 *  - config: any (optional)
 * 
 * Example configuration:
 * {
 *  "pipeline_name": "my_pipeline",
 *  "data_source": "my_database",
 *  "data_transformations": [
 *    {
 *      "type": "filter",
 *      "config": {
 *        "column": "age",
 *        "operator": ">",
 *        "value": 18
 *      }
 *    },
 *    {
 *      "type": "aggregation",
 *      "config": {
 *        "column": "sales",
 *        "function": "SUM"
 *      }
 *    }
 *  ],
 *  "data_sink": "my_data_warehouse"
 * }
 */

interface DataTransformation {
  type: string;
  config?: any;
}

interface PipelineConfig {
  pipeline_name: string;
  data_source: string;
  data_transformations?: DataTransformation[];
  data_sink: string;
}

class PipelineGenerator {
  private config: PipelineConfig;

  constructor(config: PipelineConfig) {
    this.config = config;
  }

  generatePipeline(): string {
    const pipelineCode = `
      ${this.config.pipeline_name} = (
        ${this.config.data_source} |
        ${this.getTransformationsCode()} |
        ${this.config.data_sink}
      )
    `;
    return pipelineCode;
  }

  private getTransformationsCode(): string {
    let transformationsCode = "";
    if (this.config.data_transformations) {
      transformationsCode = this.config.data_transformations.map((transformation) => {
        switch (transformation.type) {
          case "filter":
            return `filter(${transformation.config.column} ${transformation.config.operator} ${transformation.config.value})`;
          case "aggregation":
            return `aggregate(${transformation.config.column}, ${transformation.config.function})`;
          default:
            throw new Error(`Unknown transformation type: ${transformation.type}`);
        }
      }).join(" | ");
    }
    return transformationsCode;
  }
}

// Example usage
const config: PipelineConfig = {
  pipeline_name: "my_pipeline",
  data_source: "my_database",
  data_transformations: [
    {
      type: "filter",
      config: {
        column: "age",
        operator: ">",
        value: 18
      }
    },
    {
      type: "aggregation",
      config: {
        column: "sales",
        function: "SUM"
      }
    }
  ],
  data_sink: "my_data_warehouse"
};

const generator = new PipelineGenerator(config);
const pipelineCode = generator.generatePipeline();
console.log(pipelineCode);