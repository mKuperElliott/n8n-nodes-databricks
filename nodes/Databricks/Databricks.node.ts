import {
  IExecuteFunctions,
  INodeExecutionData,
  INodeType,
  INodeTypeDescription,
  NodeConnectionTypes,
  IHttpRequestMethods,
} from 'n8n-workflow';
import {
  filesOperations,
  filesParameters,
  genieOperations,
  genieParameters,
  unityCatalogOperations,
  unityCatalogParameters,
  databricksSqlOperations,
  databricksSqlParameters,
  modelServingOperations,
  modelServingParameters,
  vectorSearchOperations,
  vectorSearchParameters,
} from './resources';

interface DatabricksCredentials {
  host: string;
  token: string;
}

export class Databricks implements INodeType {
  description: INodeTypeDescription = {
      displayName: 'Databricks',
      name: 'databricks',
      icon: 'file:databricks.svg',
      group: ['transform'],
      version: 1,
      usableAsTool: true,
      subtitle: '={{$parameter["operation"] + ": " + $parameter["resource"]}}',
      description: 'Interact with Databricks API',
      defaults: {
          name: 'Databricks',
      },
      inputs: [NodeConnectionTypes.Main],
      outputs: [NodeConnectionTypes.Main],
      credentials: [
          {
              name: 'databricks',
              required: true,
          },
      ],
      requestDefaults: {
          baseURL: '={{$credentials.host}}',
          headers: {
              Authorization: '=Bearer {{$credentials.token}}',
          },
      },
      properties: [
          {
              displayName: 'Resource',
              name: 'resource',
              type: 'options',
              noDataExpression: true,
              options: [
                  { name: 'Genie', value: 'genie' },
                  { name: 'Databricks SQL', value: 'databricksSql' },
                  { name: 'Unity Catalog', value: 'unityCatalog' },
                  { name: 'Model Serving', value: 'modelServing' },
                  { name: 'Files', value: 'files' },
                  { name: 'Vector Search', value: 'vectorSearch' },
              ],
              default: 'databricksSql',
          },
          filesOperations,
          genieOperations,
          unityCatalogOperations,
          databricksSqlOperations,
          modelServingOperations,
          vectorSearchOperations,
          ...filesParameters,
          ...genieParameters,
          ...unityCatalogParameters,
          ...databricksSqlParameters,
          ...modelServingParameters,
          ...vectorSearchParameters,
      ],
  };

  // --- Databricks SQL helpers ---

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function postSqlStatement(
  ctx: IExecuteFunctions,
  host: string,
  token: string,
  body: any,
) {
  return ctx.helpers.httpRequest({
    method: 'POST',
    url: `${host}/api/2.0/sql/statements`,
    headers: {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json',
    },
    body,
    json: true,
  });
}

async function getSqlStatement(
  ctx: IExecuteFunctions,
  host: string,
  token: string,
  statementId: string,
) {
  return ctx.helpers.httpRequest({
    method: 'GET',
    url: `${host}/api/2.0/sql/statements/${encodeURIComponent(statementId)}`,
    headers: { Authorization: `Bearer ${token}` },
    json: true,
  });
}

// IMPORTANT: External chunk links are presigned storage URLs -> do
NOT send Authorization headers.
async function fetchExternalChunkJson(url: string) {
  // n8n provides a global fetch in recent versions; if not, replace
with a plain https request helper.
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Chunk fetch failed: ${res.status}
${res.statusText}`);
  return res.json();
}

function rowsToItems(
  columns: Array<{ name: string }>,
  dataArray: any[][],
): INodeExecutionData[] {
  const headers = columns.map((c) => c.name);
  return (dataArray || []).map((row) => ({
    json: Object.fromEntries(row.map((v, i) => [headers[i], v])),
  }));


  async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
      const items = this.getInputData();
      const returnData: INodeExecutionData[] = [];

      this.logger.debug(`Starting execution with ${items.length} items`);

      for (let i = 0; i < items.length; i++) {
          try {
              this.logger.debug(`Processing item ${i + 1}/${items.length}`);
              const resource = this.getNodeParameter('resource', i) as string;
              const operation = this.getNodeParameter('operation', i) as string;

              this.logger.debug('Node parameters', {
                  resource,
                  operation,
                  itemIndex: i
              });

              if (resource === 'files' && operation === 'uploadFile') {
                  const dataFieldName = this.getNodeParameter('dataFieldName', i) as string;
                  const catalog = this.getNodeParameter('catalog', i) as string;
                  const schema = this.getNodeParameter('schema', i) as string;
                  const volume = this.getNodeParameter('volume', i) as string;
                  const path = this.getNodeParameter('path', i) as string;

                  this.logger.debug('File upload parameters', {
                      dataFieldName,
                      catalog,
                      schema,
                      volume,
                      path
                  });

                  const credentials = (await this.getCredentials('databricks')) as DatabricksCredentials;
                  const host = credentials.host;
                  const binaryData = await this.helpers.getBinaryDataBuffer(i, dataFieldName);

                  this.logger.debug('Starting file upload', {
                      host,
                      path,
                      dataSize: binaryData.length
                  });

                  await this.helpers.httpRequest({
                      method: 'PUT',
                      url: `${host}/api/2.0/fs/files/Volumes/${catalog}/${schema}/${volume}/${path}`,
                      body: binaryData,
                      headers: {
                          Authorization: `Bearer ${credentials.token}`,
                          'Content-Type': items[i].binary?.[dataFieldName]?.mimeType || 'application/octet-stream',
                      },
                      encoding: 'arraybuffer',
                  });

                  this.logger.debug('File upload successful', { path });
                  returnData.push({
                      json: {
                          success: true,
                          message: `File uploaded successfully to ${path}`,
                      },
                  });
              } else if (resource === 'genie') {
                  const credentials = (await this.getCredentials('databricks')) as DatabricksCredentials;
                  const host = credentials.host;

                  let url: string;
                  let method: IHttpRequestMethods;
                  let body: object | undefined;

                  switch (operation) {
                      case 'createMessage':
                          url = `${host}/api/2.0/genie/spaces/${
                              this.getNodeParameter('spaceId', i) as string
                          }/conversations/${
                              this.getNodeParameter('conversationId', i) as string
                          }/messages`;
                          method = 'POST';
                          body = {
                              content: this.getNodeParameter('message', i) as string,
                          };
                          break;

                      case 'getMessage':
                          url = `${host}/api/2.0/genie/spaces/${
                            this.getNodeParameter('spaceId', i) as string
                        }/conversations/${
                            this.getNodeParameter('conversationId', i) as string
                        }/messages/${
                              this.getNodeParameter('messageId', i) as string
                          }`;
                          method = 'GET';
                          break;

                      case 'getQueryResults':
                          url = `${host}/api/2.0/genie/spaces/${
                            this.getNodeParameter('spaceId', i) as string
                        }/conversations/${
                            this.getNodeParameter('conversationId', i) as string
                        }/messages/${
                            this.getNodeParameter('messageId', i) as string
                        }/attachments/${
                            this.getNodeParameter('attachmentId', i) as string
                        }/query-result`;
                          method = 'GET';
                          break;

                      case 'executeMessageQuery':
                          url = `${host}/api/2.0/genie/spaces/${
                            this.getNodeParameter('spaceId', i) as string
                        }/conversations/${
                            this.getNodeParameter('conversationId', i) as string
                        }/messages/${
                            this.getNodeParameter('messageId', i) as string
                        }/attachments/${
                            this.getNodeParameter('attachmentId', i) as string
                        }/execute-query`;
                          method = 'POST';
                          break;

                      case 'getSpace':
                          url = `${host}/api/2.0/genie/spaces/${
                            this.getNodeParameter('spaceId', i) as string
                          }`;
                          method = 'GET';
                          break;

                      case 'startConversation':
                        const spaceId = this.getNodeParameter('spaceId', i) as string;
                        url = `${host}/api/2.0/genie/spaces/${spaceId}/start-conversation`;
                          method = 'POST';
                          body = {
                            content: this.getNodeParameter('initialMessage', i) as string,
                          };
                          break;

                      default:
                          throw new Error(`Unsupported Genie operation: ${operation}`);
                  }

                  this.logger.debug('Making Genie API request', {
                      url,
                      method,
                      body: JSON.stringify(body, null, 2)
                  });

                  const response = await this.helpers.httpRequest({
                      method,
                      url,
                      body,
                      headers: {
                          Authorization: `Bearer ${credentials.token}`,
                          'Content-Type': 'application/json',
                      },
                      json: true,
                  });

                  this.logger.debug('Genie API response received', {
                      statusCode: response.statusCode,
                      response: JSON.stringify(response, null, 2)
                  });

                  returnData.push({ json: response });
              } else {
                  this.logger.debug('Passing through unhandled resource', { resource });
                  returnData.push({
                      json: items[i].json,
                  });
              }
          } catch (error) {
              const currentResource = this.getNodeParameter('resource', i) as string;
              const currentOperation = this.getNodeParameter('operation', i) as string;
              
              this.logger.error(`Error processing item ${i + 1}`, {
                  error: error.message,
                  stack: error.stack,
                  itemIndex: i,
                  resource: currentResource,
                  operation: currentOperation
              });

              if (error.response) {
                  // API Error
                  this.logger.error('API Error', {
                      status: error.response.status,
                      statusText: error.response.statusText,
                      data: error.response.data
                  });
                  if (this.continueOnFail()) {
                      returnData.push({ 
                          json: { 
                              error: `API Error: ${error.response.status} ${error.response.statusText}`, details: error.response.data
                          } 
                      });
                  } else {
                      throw new Error(`API Error: ${error.response.status} ${error.response.statusText}`);
                  }
              } else if (error.request) {
                  // Network Error
                  this.logger.error('Network Error', {
                      request: error.request
                  });
                  if (this.continueOnFail()) {
                      returnData.push({ 
                          json: { 
                              error: 'Network Error: No response received from server',
                              details: error.message
                          } 
                      });
                  } else if (resource === 'databricksSql') {
                      const credentials = (await this.getCredentials('databricks')) as DatabricksCredentials;
                      const host = credentials.host;
                    
                      if (operation === 'executeQuery') {
                        // --- Read node params (your UI uses "query" for SQL text, plus warehouseId & additionalFields.*) ---
                        const warehouseId = this.getNodeParameter('warehouseId', i) as string;
                        const sql = this.getNodeParameter('query', i) as string;
                    
                        // optional collection
                        const additional = (this.getNodeParameter('additionalFields', i, {}) || {}) as Record<string, any>;
                        const catalog = (additional.catalog as string) || undefined;
                        const schema = (additional.schema as string) || undefined;
                    
                        // UI gives a number (seconds). Databricks API expects a duration string like "60s".
                        const timeoutNum = typeof additional.timeout === 'number' ? additional.timeout : 60;
                        const waitTimeout = `${timeoutNum}s`;
                    
                        // --- Build request body for POST /api/2.0/sql/statements ---
                        const body: any = {
                          warehouse_id: warehouseId,
                          statement: sql,
                          // INLINE is simplest; if you expect very large results, switch to "EXTERNAL_LINKS".
                          disposition: 'INLINE',
                          wait_timeout: waitTimeout,
                        };
                        if (catalog) body.catalog = catalog;
                        if (schema) body.schema = schema;
                    
                        this.logger.debug('Databricks SQL submit body', body);
                    
                        // --- Submit the statement ---
                        const submitted = await postSqlStatement(this, host, credentials.token, body);
                        const statementId: string | undefined = submitted?.statement_id;
                        if (!statementId) {
                          throw new Error(`No statement_id returned from Databricks. Response: ${JSON.stringify(submitted)}`);
                        }
                    
                        // --- Poll for completion ---
                        const startedAt = Date.now();
                        const maxOverallMs = 5 * 60 * 1000; // hard stop after 5 minutes
                        let backoff = 500;                  // 0.5s initial poll interval
                        let current = submitted;
                    
                        // Databricks returns state in different shapes depending on rev; normalize:
                        const readState = (obj: any): string | undefined => obj?.status?.state || obj?.state?.status || obj?.state;
                    
                        while (true) {
                          const state = readState(current);
                          if (state === 'SUCCEEDED') break;
                    
                          if (['FAILED', 'CANCELED', 'CLOSED'].includes(state || '')) {
                            const errPayload = current?.error || current?.status?.error || current;
                            throw new Error(`Statement ${state}. Details: ${JSON.stringify(errPayload)}`);
                          }
                    
                          if (Date.now() - startedAt > maxOverallMs) {
                            throw new Error(`Timed out waiting for statement ${statementId}`);
                          }
                    
                          await sleep(backoff);
                          backoff = Math.min(backoff * 1.5, 5000);
                          current = await getSqlStatement(this, host, credentials.token, statementId);
                        }
                    
                        // --- Extract results into n8n items ---
                        const items: INodeExecutionData[] = [];
                        const result = current?.result;
                    
                        if (!result) {
                          // DDL or statements without a rowset -> return empty but successful
                          returnData.push({ json: { success: true, rowCount: 0 } });
                        } else if (result.data_array && result.schema?.columns) {
                          // INLINE disposition
                          items.push(...rowsToItems(result.schema.columns, result.data_array));
                          returnData.push(...items);
                        } else if (current?.manifest?.total_chunk_count || (result.external_links && result.external_links.length)) {
                          // EXTERNAL_LINKS disposition (or mixed)
                          const chunks = result.external_links as string[]; // presigned URLs
                          for (const link of chunks) {
                            const chunk = await fetchExternalChunkJson(link);
                            items.push(...rowsToItems(chunk.schema.columns, chunk.data_array));
                          }
                          returnData.push(...items);
                        } else {
                          // Unexpected shape; surface the payload for debugging
                          returnData.push({ json: { warning: 'No rows found and no external links', raw: current } });
                        }
                      } else {
                        // If you advertise more operations under Databricks SQL later, add them here:
                        throw new Error(`Unsupported Databricks SQL operation: ${operation}`);
                      }

                  } else {
                      throw new Error('Network Error: No response received from server');
                  }
              } else {
                  // Other Error
                  if (this.continueOnFail()) {
                      returnData.push({ 
                          json: { 
                              error: error.message,
                              details: error.stack
                          } 
                      });
                  } else {
                      throw error;
                  }
              }
          }
      }

      this.logger.debug('Execution completed successfully');
      return [returnData];
  }
}
