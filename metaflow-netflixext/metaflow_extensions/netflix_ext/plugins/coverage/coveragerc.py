# this file is named coveragerc.py since metaflow does not have a way to package hidden files

[run]
branch = True
parallel = True
concurrency = multiprocessing,thread

[html]
show_contexts = True

[report]
omit =
  /apps/bdi-venv*/*
  /**/site-packages/*
  /**/vendor/*
  /**/vendored/*
  /**/_vendor/*
  /**/build/*
  /**/pip-install-*/*
  /**/pip-req-build-*/*
  /**/_remote_module_non_scriptable.py
  /**/__autograph_generated_*.py
  /**/metaflow-function-*/*
  metaflow/plugins/airflow/*
  metaflow/plugins/argo/*
  metaflow/plugins/azure/*
  metaflow/plugins/gcp/*
  metaflow/plugins/kubernetes/*
  coveragerc.py
  setup.py

include_namespace_packages = True
show_missing = False
ignore_errors = False

[paths]
; Specific paths must come before general paths
; /root/metaflow/.mf_code/metaflow_extensions is more specific than /root/metaflow
metaflow_extensions =
    .mf_code/metaflow_extensions
    metaflow_extensions
    nflx-metaflow-functions/metaflow_extensions
    nflx-metaflow-serving/metaflow_extensions
    nflx-metaflow/metaflow_extensions
    nflx-fastdata/metaflow_extensions
    mli-metaflow-custom-repo/nflx-metaflow-functions/metaflow_extensions
    mli-metaflow-custom-repo/nflx-metaflow-serving/metaflow_extensions
    mli-metaflow-custom-repo/nflx-metaflow/metaflow_extensions
    mli-metaflow-custom-repo/nflx-fastdata/metaflow_extensions
    metaflow-repo/test/extensions/packages/card_via_extinit/metaflow_extensions
    metaflow-repo/test/extensions/packages/card_via_init/metaflow_extensions
    metaflow-repo/test/extensions/packages/card_via_ns_subpackage/metaflow_extensions
    /*/.mf_code/metaflow_extensions
    /*/*/.mf_code/metaflow_extensions
    /**/.mf_code/metaflow_extensions
    /root/metaflow/.mf_code/metaflow_extensions
    /data/tmp/*/env/install/lib/*/site-packages/metaflow_extensions
    /apps/*/lib/*/site-packages/metaflow_extensions

; Test flow files are flattened into test_flows_flat to match remote execution structure
; First line is canonical location (where files exist), rest are aliases (execution paths)
test_flows_flat =
    .mf_code/test_flows_flat
    /root/metaflow
    /*/metaflow

.mf_code =
    .mf_code
    /*/.mf_code
    /*/*/.mf_code
    /**/.mf_code

metaflow =
    .mf_code/metaflow
    metaflow
    metaflow-repo/metaflow
    /*/.mf_code/metaflow
    /*/*/.mf_code/metaflow
    /**/.mf_code/metaflow
    /*/metaflow
    /*/*/metaflow
    /**/metaflow
    /data/tmp/*/env/install/lib/*/site-packages/metaflow
    /apps/*/lib/*/site-packages/metaflow

test =
    .mf_code/test
    test
    mli-metaflow-custom-repo/test
    metaflow-repo/test
    /*/metaflow/test
    /*/*/metaflow/test
    /**/metaflow/test

; Spin tests are at test/unit/spin in OSS metaflow but executed at test/plugins/spin/spin
test_spin =
    .mf_code/test/unit/spin
    /root/metaflow/test/plugins/spin/spin
    /*/metaflow/test/plugins/spin/spin

cwd =
    .
    mli-metaflow-custom-repo
    /data/tmp/*/user/corp/mli-metaflow-custom
    /*/metaflow
    /*/*/metaflow
    /**/.mf_code
    /**/metaflow
