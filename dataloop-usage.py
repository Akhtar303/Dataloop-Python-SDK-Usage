import json
import datetime
import dtlpy as dl
from dtlpy.entities import Polygon, Box, Point, Classification


# Initialize SDK
dl.login() 

class Dataloop:

  def __init__(self, project_name, dataset_name):
    self.project = dl.projects.get(project_name)  
    self.dataset = self.project.datasets.get(dataset_name)

    # Add labels
    labels = ["1","2", "3","top", "bottom"]
    for label in labels:
      self.dataset.add_label(label_name=label)
  # uploading annotations on Dataloop
  def upload_images(self, image_paths):
    for image_path in image_paths:
      item = self.dataset.items.upload(local_path=image_path)

      # Add metadata
      current_time_utc = datetime.datetime.utcnow()
      metadata = {"collected": current_time_utc.isoformat()}
      item.metadata = metadata
      item.update()

# uploading annotations on Dataloop
  def upload_annotations(self, annotations_data):
    for image_name, label_data in annotations_data.items():
        item = self.dataset.items.get('/' + image_name)
        builder = item.annotations.builder()
        for labels_data in label_data:

            if 'polygon' in labels_data:
                polygon = labels_data['polygon']
                # Build formatted polygon 
                formatted_polygon = []
                for pair in polygon:
                    coord_pair = []
                    for pt in pair:
                        x = pt['x']
                        y = pt['y']

                        try:
                            x = float(x)
                            y = float(y) 
                            coord_pair.append([x, y])

                        except ValueError as e:
                            print("Error parsing coordinates: ", e)
                            raise # or other handling

                    formatted_polygon.append(coord_pair)
                flattened = [coord for pair in formatted_polygon for coord in pair]
                # Pass the flattened list to Polygon
                annotation = dl.Polygon(geo=flattened, label=labels_data['label'])

            elif 'box' in labels_data:

                bbox = labels_data['box']
                x1 = bbox[0]['x'] 
                y1 = bbox[0]['y']
                x2 = bbox[1]['x']
                y2 = bbox[1]['y']

                annotation = Box(label=labels_data['label'],
                                top=y1, left=x1,
                                bottom=y2, right=x2)
            
            elif 'point' in labels_data:

                point = labels_data['point']
                annotation = Point(x=point['x'],
                                    y=point['y'],
                                    label=labels_data['label'])
                
            elif 'classification' in labels_data:

                annotation = Classification(label=labels_data['label'])
            else:
                    annotation = None

            if annotation:  
                builder.add(annotation)
            item.annotations.upload(builder)
    return 'sucessfully upload annotations on Dataloop '

#filter data on the basiss of label
  def filter_by_label(self, label):
    filter = dl.Filters()
    filter.add_join(field='label', values=label) 
    pages = self.dataset.items.list(filters=filter)
    for item in pages.all():
        print(f'Item: {item.name}')
        print(f'ID: {item.id}')
    return 'sucessfully filter data on the basiss of label'

#filter data on the basiss of annotation type
  def filter_annotations(self, annotation_type):
    annotation_filters = dl.Filters(resource=dl.FiltersResource.ANNOTATION)
    annotation_filters.add(field='type', values=annotation_type)
    pages = self.dataset.annotations.list(filters=annotation_filters)
    for page in pages:
        for annotation in page:

            print(f'Item: {annotation.item.name} ({annotation.item.id})')

            print(f'Annotation id: {annotation.id}')
            print(f'Annotation label: {annotation.label}')
            print(f'Point x: {annotation.coordinates["x"]}')
            print(f'Point y: {annotation.coordinates["y"]}')
    return 'sucessfully filter data on the basiss of annotation type'

# project name and dataset name
project_name = 'ML-demo-project'
dataset_name = 'cv-dataset'

#  creating object instances of Dataloop class
dataloop = Dataloop(project_name, dataset_name) 
dataloop.upload_images(['1.jpg', '2.jpg','3.jpg'])
print('Images have uploaded successfully on dataloop')

# loading annotations file
with open('annotations.json') as f:
  annotations = json.load(f)

print('annotations file loaded succesfully')
dataloop.upload_annotations(annotations) 
print('Annotations have uploaded successfully on dataloop')

# filtering data on the basis of label
print(dataloop.filter_by_label('top'))
# filtering data on the basis annotation type
print(dataloop.filter_annotations('point'))
  

