import {
  app,
  HttpHandler,
  HttpRequest,
  HttpResponse,
  InvocationContext,
} from "@azure/functions";
import {
  BlobServiceClient,
  StorageSharedKeyCredential,
} from "@azure/storage-blob";
import * as df from "durable-functions";
import {
  ActivityHandler,
  OrchestrationContext,
  OrchestrationHandler,
} from "durable-functions";
import * as fs from "fs";
import * as JSZip from "jszip";

// "http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"

const sharedKeyCredential = new StorageSharedKeyCredential(
  "devstoreaccount1",
  "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
);

const blobServiceClient = new BlobServiceClient(
  "http://127.0.0.1:10000/devstoreaccount1",
  sharedKeyCredential
);

const exportFileTriggerOrchestrator: OrchestrationHandler = function* (
  context: OrchestrationContext
) {
  context.df.setCustomStatus("Preparing  LO list");
  const listOfLos = yield context.df.callActivity("getListOfLos", null);
  console.log(`listOfLos: ${JSON.stringify(listOfLos)}`);

  context.df.setCustomStatus("Exporting files");
  const tasks: df.Task[] = [];
  for (const lo of listOfLos) {
    tasks.push(
      context.df.callActivity("exportFileTrigger", {
        loName: lo.name,
        fileList: lo.fileList,
      })
    );
  }

  const results = yield context.df.Task.all(tasks);

  yield context.df.callActivity("sendMailToCustomer", "Export done");

  return results;
};
df.app.orchestration(
  "exportFileTriggerOrchestrator",
  exportFileTriggerOrchestrator
);

const fakeSleep = (ms: number) =>
  new Promise((resolve) => setTimeout(resolve, ms));

const sendMailToCustomer: ActivityHandler = async (input: string) => {
  console.log(`sendMailToCustomer: ${input}`);
  await fakeSleep(5000);
  console.log(`sendMailToCustomer: ${input} done`);
  return `Mail sent to ${input}`;
};

const getListOfLos: ActivityHandler = async () => {
  return [
    {
      name: "LO1",
      id: 1,
      fileList: ["large-file.json", "large-file.json", "large-file.json"],
    },
    {
      name: "LO2",
      id: 2,
      fileList: ["large-file.json", "large-file.json"],
    },
    {
      name: "LO3",
      id: 3,
      fileList: [
        "large-file.json",
        "large-file.json",
        "large-file.json",
        "large-file.json",
        "large-file.json",
        "large-file.json",
        "large-file.json",
        "large-file.json",
        "large-file.json",
        "large-file.json",
        "large-file.json",
        "large-file.json",
        "large-file.json",
        "large-file.json",
        "large-file.json",
        "large-file.json",
        "large-file.json",
        "large-file.json",
        "large-file.json",
        "large-file.json",
        "large-file.json",
        "large-file.json",
        "large-file.json",
        "large-file.json",
        "large-file.json",
        "large-file.json",
        "large-file.json",
        "large-file.json",
        "large-file.json",
        "large-file.json",
        "large-file.json",
        "large-file.json",
      ],
    },
  ];
};

const exportFileTrigger: ActivityHandler = async (
  props: {
    loName: string;
    fileList: string[];
  },
  context
) => {
  console.log(`exportFileTrigger: ${props.loName}`);
  //get file.csv from blob storage
  const containerClient = blobServiceClient.getContainerClient("fake-data");
  const results = await Promise.all(
    props.fileList.map(async (file) => {
      const blobClient = containerClient.getBlobClient(file);
      const downloadBlockBlobResponse = await blobClient.download();

      return downloadBlockBlobResponse;
    })
  );

  const zip = new JSZip();
  const zipName = `${props.loName}-${new Date().toISOString()}.zip`;
  results.forEach((result, index) => {
    zip.file(`file-${index + 1}.json`, result.readableStreamBody);
  });
  const zipFile = await zip.generateNodeStream();

  //store zip file in blob storage
  const result = await containerClient
    .getBlockBlobClient(zipName)
    .uploadStream(zipFile as any);
  console.log(JSON.stringify(result._response.status));
};
df.app.activity("exportFileTrigger", { handler: exportFileTrigger });

df.app.activity("getListOfLos", { handler: getListOfLos });

df.app.activity("sendMailToCustomer", { handler: sendMailToCustomer });

const exportFileTriggerHttpStart: HttpHandler = async (
  request: HttpRequest,
  context: InvocationContext
): Promise<HttpResponse> => {
  const client = df.getClient(context);
  const body: unknown = await request.text();

  const instanceId: string = await client.startNew(
    request.params.orchestratorName,
    { input: body }
  );

  context.log(`Started orchestration with ID = '${instanceId}'.`);

  return client.createCheckStatusResponse(request, instanceId);
};

app.http("exportFileTriggerHttpStart", {
  route: "orchestrators/{orchestratorName}",
  extraInputs: [df.input.durableClient()],
  handler: exportFileTriggerHttpStart,
});
