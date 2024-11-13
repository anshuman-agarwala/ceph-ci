pipeline {
  agent any
  stages {
    stage("build") {
      steps {
        script {
          buildName nameTemplate: "#${BUILD_NUMBER} ${BRANCH}, ${GIT_COMMIT}, ${DISTROS}, ${FLAVOR}"
          build(
            job: "ceph-dev-new-build",
            parameters: [
              string(name: "BRANCH", value: env.BRANCH),
              string(name: "DISTROS", value: env.DISTROS),
              string(name: "ARCHS", value: env.ARCHS),
              booleanParam(name: "THROWAWAY", value: env.THROWAWAY),
              booleanParam(name: "FORCE", value: env.FORCE),
              string(name: "FLAVOR", value: env.FLAVOR),
              string(name: "CI_CONTAINER", value: env.CI_CONTAINER),
              booleanParam(name: "DWZ", value: env.DWZ),
              booleanParam(name: "SCCACHE", value: env.SCCACHE),
            ]
          )
        }
      }
    }
  }
}
