import React from 'react'
import PropTypes from 'prop-types'
import { Grid } from 'semantic-ui-react'

import { validators } from 'shared/components/form/FormHelpers'
import FormWrapper from 'shared/components/form/FormWrapper'
import SubmitFormPage from 'shared/components/page/SubmitFormPage'
import { GENOME_VERSION_FIELD } from 'shared/utils/constants'

const FIELDS = [
  { validate: validators.required, ...GENOME_VERSION_FIELD },
]

const GeneVariantLookupLayout = ({ fields, uploadStats, onSubmit }) => (
  <Grid divided="vertically">
    <Grid.Row>
      <Grid.Column width={4} />
      <Grid.Column width={8}>
        <FormWrapper
          onSubmit={onSubmit}
          fields={fields}
          noModal
          showErrorPanel
        />
      </Grid.Column>
      <Grid.Column width={4} />
    </Grid.Row>
    <Grid.Row>
      <Grid.Column width={16}>
        {JSON.stringify(uploadStats)}
      </Grid.Column>
    </Grid.Row>
  </Grid>
)

GeneVariantLookupLayout.propTypes = {
  fields: PropTypes.arrayOf(PropTypes.object),
  uploadStats: PropTypes.object,
  onSubmit: PropTypes.func,
}

const GeneVariantLookup = () => (
  <SubmitFormPage
    fields={FIELDS}
    url="/api/gene_variant_lookup"
    header="Lookup Variants in Gene"
    formClass={GeneVariantLookupLayout}
  />
)

export default GeneVariantLookup
