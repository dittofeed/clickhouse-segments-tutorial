import fs from "fs/promises";
import path from "path";
import { createClient } from "@clickhouse/client";

describe("example", () => {
  beforeAll(async () => {
    const sql = await fs.readFile(path.join(__dirname, "naive.sql"), "utf-8");
    const ch = createClient({
      host: "http://localhost:8123",
      database: "segmentation",
    });
    console.log("loc1", sql);
  });

  it("should be true", () => {
    expect(true).toBe(true);
  });

  it("should be false", () => {
    expect(false).toBe(true);
  });
});
