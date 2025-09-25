import scripts.ingest as ingest
import scripts.dq_pre_transform as dq_pre_transform
import scripts.transform as transform
import scripts.dq_post_transform as dq_post_transform
import scripts.model as model

if __name__ == "__main__":
    print("Starting pipeline. Ingestion Starts Now ...")
    ingest.ingest()
    print("Bronze layer successfully created.")
    print("")
    #dq_pre_transform.dq_checks()
    print("DQ pre transform skipped/passed. Starting Transforming Data Now ...")
    print("")
    transform.transform()
    print("Silver layer successfully created. DQ Starting Now ...")
    print("")
    dq_post_transform.dq_checks()
    print("DQ post transform passed. Final Models Are getting Loaded Now ...")
    print("")
    model.model()
    print("Pipeline completed successfully")
    print("")
