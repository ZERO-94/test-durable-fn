import {
  app,
  HttpHandler,
  HttpRequest,
  HttpResponse,
  InvocationContext,
} from "@azure/functions";
import * as df from "durable-functions";
import {
  ActivityHandler,
  OrchestrationContext,
  OrchestrationHandler,
} from "durable-functions";

const activityName = "exportFileForSingleLo";

const exportFileForSingleLoOrchestrator: OrchestrationHandler = function* (
  context: OrchestrationContext
) {
  const outputs = [];
  outputs.push(yield context.df.callActivity(activityName, "Tokyo"));
  outputs.push(yield context.df.callActivity(activityName, "Seattle"));
  outputs.push(yield context.df.callActivity(activityName, "Cairo"));

  return outputs;
};
df.app.orchestration(
  "exportFileForSingleLoOrchestrator",
  exportFileForSingleLoOrchestrator
);

const exportFileForSingleLo: ActivityHandler = (input: string): string => {
  return `Hello, ${input}`;
};
df.app.activity(activityName, { handler: exportFileForSingleLo });

const getListOfLos = () => {
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
      ],
    },
  ];
};

const exportFileForSingleLoHttpStart: HttpHandler = async (
  request: HttpRequest,
  context: InvocationContext
): Promise<HttpResponse> => {
  const client = df.getClient(context);
  const body: unknown = await request.text();

  //get the list of LOs
  const listOfLos = getListOfLos();

  // Start the orchestration
  let instances = [];
  for (const lo of listOfLos) {
    instances = await Promise.all(
      listOfLos.map((lo) =>
        client.startNew("exportFileForSingleLoOrchestrator", {
          input: lo,
        })
      )
    );
  }

  console.log(instances.map((i) => i.instanceId));

  return new HttpResponse({
    status: 200,
    jsonBody: instances.map((i) => client.createHttpManagementPayload(i)),
  });
};

app.http("exportFileForSingleLoHttpStart", {
  route: "orchestrators/test/{orchestratorName}",
  extraInputs: [df.input.durableClient()],
  handler: exportFileForSingleLoHttpStart,
});
