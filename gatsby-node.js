const createNodeHelpers = require('gatsby-node-helpers').default;
const fetch = require('node-fetch');
const queryString = require('query-string');

const wait = (ms) => new Promise((res) => setTimeout(res, ms));

const callWithRetry = async (fnName, fn, depth = 0) => {
  try {
    return await fn();
  } catch (e) {
    if (depth > 5) {
      console.error('Attempt limit reached, impossible to fetch');
      throw e;
    }
    console.error(e);
    console.info(
      `Impossible to fetch the data from ${fnName} during the attempt number ${
        depth + 1
      }, trying again in ${20 + 10 * depth}s`
    );
    await wait(40000 + 10000 * depth);

    return callWithRetry(fn, depth + 1);
  }
};

const { createNodeFactory } = createNodeHelpers({
  typePrefix: `Marketo`,
});

async function authenticate(authUrl) {
  const res = await fetch(authUrl, {});

  if (res.ok) {
    const { access_token } = await res.json();

    return access_token;
  } else {
    throw new Error('Wrong credentials');
  }
}

exports.sourceNodes = async ({ actions, createNodeId }, configOptions) => {
  const { createNode } = actions;
  const { munchkinId, clientId, clientSecret } = configOptions;
  const authOptions = queryString.stringify({
    grant_type: 'client_credentials',
    client_id: clientId,
    client_secret: clientSecret,
  });

  const formsApiUrl = `https://${munchkinId}.mktorest.com/rest/asset/v1/forms.json?maxReturn=200`;
  const authUrl = `https://${munchkinId}.mktorest.com/identity/oauth/token?${authOptions}`;

  try {
    const accessToken = await authenticate(authUrl);

    const fetchForms = async (depth = 0) => {
      const allForms = await fetch(formsApiUrl, {
        headers: { Authorization: `Bearer ${accessToken}` },
      })
        .then((response) => {
          console.info('Response fetched!');
          return response.json();
        })
        .then(async (data) => {
          if (!data.success) {
            if (depth > 5) {
              console.error(
                'Impossible to fetch the forms, maximum number of attempts reached'
              );
              throw e;
            }
            console.error(
              `Impossible to fetch the forms content during the attempt number ${
                depth + 1
              }, trying again in ${20 + 10 * depth}s`
            );
            console.error(`Error code : `, data?.errors[0]?.code);
            console.error(`Error message : `, data?.errors[0]?.message);
            await wait(20000 + 10000 * depth);

            fetchForms(depth + 1);
          } else {
            console.info('All forms correctly fetched');
            return data;
          }
        })
        .catch((error) => {
          console.error('Error trying to fetch the forms >>>> ', error);
        });

      return allForms;
    };

    const forms = await fetchForms();

    async function fetchFormFields(id) {
      const url = `https://${munchkinId}.mktorest.com/rest/asset/v1/form/${id}/fields.json`;

      const results = await fetch(url, {
        headers: { Authorization: `Bearer ${accessToken}` },
      })
        .then((res) => {
          return res.json();
        })
        .catch((error) => {
          console.error(
            `Error trying to fetch the form id ${id} fields >>>> `,
            error
          );
        });
      
      if(results.errors) {
        results.errors.map((error) => {
          console.log(`${error.code}: ${error.message}`)
        })
        throw Error();
      }

      return results;
    }

    await Promise.all(
      forms.result.map(async (form) => {
        const { result: children } = await callWithRetry(
          'fetchFormFields',
          () => fetchFormFields(form.id)
        );
        const Form = createNodeFactory('Form')({
          ...form,
          children,
        });
        createNode(Form);
        
        console.info(`Node created for form ID >>>> ${Form.id}: ${Form.name}`);

        if(!Form.marketoChildren) {
          console.error(`${Form.id} has no fields!`);
        }
      })
    )
      .then(() => {
        console.info('Form and fields successfully fetched');
      })
      .catch((error) => {
        console.error(
          'Error during the Promise.all phase >>>> ',
          error.message
        );
      });
  } catch (err) {
    console.error('gatsby-source-marketo-forms:', err.message);
  }
};
